  #set page(
    paper: "a4",
    margin: (x: 2cm, top: 2.5cm, bottom: 2.5cm),
    header: none,
    footer: context [
      #let page_number = counter(page).get().first()
      #let total_pages = counter(page).final().first()

      #if page_number == 1 {
        return none
      }

      #align(right)[
        #text(12pt, fill: rgb("#1a2b4c"))[#page_number з #total_pages]
      ]
    ]
  )

  #set list(marker: [---])

  #show figure.caption: it => [
    #text(it.supplement + " " + it.counter.display(it.numbering))
    ---
    #it.body
  ]

  #set text(font: "Times New Roman", size: 14pt, lang: "uk", weight: "regular")
  #set par(justify: true, leading: 0.65em, first-line-indent: (amount: 1.25em, all: true))

  #set heading(numbering: "1.1")

  #show heading: it => {
    set text(fill: rgb("#1a2b4c"))
    block(below: 1em, above: 1.2em, it)
  }

  // Code Block Styling
  #show raw.where(block: true): it => [
    #block(
      fill: rgb("#f4f5f7"),
      inset: 10pt,
      radius: 4pt,
      width: 100%,
      stroke: 0.5pt + rgb("#e0e0e0"),
      it
    )
  ]

  #show raw.where(block: false): it => [
    #box(
      fill: rgb("#f4f5f7"),
      inset: 2pt,
      radius: 2pt,
      stroke: 0.5pt + rgb("#e0e0e0"),
      text(fill: rgb("#1a2b4c"))[#it]
    )
  ]


  // Header Box
  #align(center + horizon)[
    #block(width: 100%)[
      #text(24pt, weight: "bold")[Фінальний проєкт] \
      #v(2pt)
      #text[з дисципліни «Аналіз великих даних (Big Data)»] \
      #v(8pt)
      #text(16pt)[Тема: «Виявлення зон із найбільшими аномаліями тривалості поїздок відносно погодинної норми маршруту»] \
      #v(160pt)
      #align(right)[
        #text[Виконав] \
        #text[Студент МП ІПЗ-1] \
        #text[Остапенко Дмитро]
      ]
    ]
  ]

#pagebreak()

= Задача та датасет

== Постановка задачі

Виявити зони прибуття (`DOLocationID`) у Нью-Йорку з найбільшою транспортною нестабільністю — часткою поїздок, чия тривалість суттєво перевищує медіану для відповідного маршруту `(PULocationID, DOLocationID)` та години доби (`hour_of_day`). Відповідь неможливо отримати за один прохід над сирими даними: спочатку потрібно сформувати еталонну тривалість поїздок для кожного маршруту й години доби, потім зіставити кожну поїздку з цим еталоном, обчислити відхилення та виконати повторну агрегацію за зоною прибуття (`DOLocationID`).

== Характеристики датасету
- **Джерело:** NYC TLC Trip Record Data — Yellow Taxi Trip Records (12 місячних файлів, об'єднаних у нестиснений CSV).
- **Посилання:** `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- **Ліцензія:** Open Data / Public Domain (NYC Open Data Terms of Use / CC0); дозволено безплатне навчальне та дослідницьке використання.
- **Розмір без стиснення:** `4.8 ГБ` (контрольна сума `sha256sum` фіксується у `README.md` та артефактах).
- **Кількість записів:** `~49 млн` рядків поїздок.
- **Ключові поля схеми:** `tpep_pickup_datetime` (timestamp), `tpep_dropoff_datetime` (timestamp), `passenger_count` (numeric), `trip_distance` (float), `PULocationID` (int), `DOLocationID` (int), `fare_amount` (float), `total_amount` (float).
- **Середовище:** Apache Spark (PySpark через Docker Compose із зафіксованими версіями образів), ноутбук із `16 ГБ` оперативної пам'яті.

= Алгоритм та оцінка вартості (до написання коду)

Припущення до запуску:
- Вхід: `~49 млн` записів, `4.8 ГБ` CSV (`~100` байт на рядок у тексті).
- Після локальної фільтрації валідних поїздок: `~46.5 млн` (`~95%`).
- Активні зони таксі: `~263` зони (`PU`, `DO`), `24` години доби (`0..23`).
- Кількість заповнених комбінацій `(PULocationID, DOLocationID, hour_of_day)`: оціночно `0.7M–1.1M` груп.

== Схема конвеєра

#block(stroke: 0.5pt + gray, inset: 8pt)[
  ```text
  Raw CSV (~49M рядків, 4.8 ГБ)
         │
         ▼
  [0. Фільтрація й проєкція]  (без shuffle) -> ~46.5M валідних поїздок
         │
         ├────────────────────────────────────┐
         ▼ (Shuffle 1: ключ (PU, DO, hour))   │
  [1. Норма маршруту й години]                │
     -> медіанна тривалість (~0.9M рядків)    │
         │                                    │
         ▼                                    │
  [2. Join з нормою + розрахунок ratio] ◄─────┘
     -> коефіцієнт відхилення для кожної поїздки (~46.5M рядків)
         │
         ▼ (Shuffle 2: ключ DOLocationID)
  [3. Агрегація по зоні прибуття]
     -> частка затримок та середній ratio по DOLocationID (~263 рядки)
     ```
]

== Таблиця етапів

#table(
  columns: (1.5fr, 1.3fr, 1.3fr, 1.3fr, 1.3fr),
  inset: 5pt,
  [*Параметр*],
  [*0. Фільтрація*],
  [*1. Норма маршруту*],
  [*2. Join + ratio*],
  [*3. Агрегація по DO*],

  [*Вхід (ключ, значення)*],
  [`row_id`, `csv_line`],
  [`(PU, DO, hour)`, `dur_sec`],
  [`(PU, DO, hour)` + норма],
  [`DOLocationID`, `(is_delayed, ratio)`],

  [*Вихід (ключ, значення)*],
  [`(PU, DO, hour)`, `(dur_sec, dist_mi)`],
  [`(PU, DO, hour)`, `(med_dur, cnt)`],
  [`DOLocationID`, `(is_delayed, ratio)`],
  [`DOLocationID`, `(delay_rate, avg_ratio)`],

  [*Що йде в shuffle*],
  [Немає],
  [Ключ `(PU, DO, hour)` і `dur_sec` або бакети],
  [Залежить від плану join; далі ключ `DOLocationID`],
  [Пари `(DOLocationID, (is_delayed, ratio))`],

  [*Оцінка записів і байтів*],
  [Вхід: ~49M (4.8 ГБ). Вихід: ~46.5M (~1.6–2.0 ГБ)],
  [In: ~46.5M (~1.0–1.4 ГБ). Out: ~0.9M (~40–60 МБ)],
  [Норми: ~0.9M (~50 МБ). Поїздки: ~46.5M (~1.5–2.0 ГБ)],
  [In: ~46.5M (~1.1–1.5 ГБ). Out: ~263 (~15–25 КБ)],

  [*Факт*],
  [—],
  [—],
  [—],
  [—],

  [*Stage / вершина / job*],
  [—],
  [—],
  [—],
  [—],
)

== Обґрунтування ключів та часткової агрегації (combiner / map-side)

- Етап 1 — ключ (PULocationID, DOLocationID, hour_of_day): поєднує напрямок поїздки та годину доби, щоб коректно відокремити короткі локальні поїздки від міжрайонних і нічний трафік від пікового. Точна медіана не згортається простою сумою, але до shuffle виконується фільтрація та локальна підготовка/наближена квантильна оцінка (percentile_approx або бакети), що зменшує обсяг переданих даних.

- Етапи 2 і 3 — ключ DOLocationID: після обчислення відносного відхилення кожної поїздки від норми маршруту агрегуємо показники по зонах прибуття. Лічильники поїздок, затримок і суми коефіцієнтів відхилення є асоціативними та комутативними, тому map-side агрегація ефективно стискає дані до ~263 ключів на партицію.