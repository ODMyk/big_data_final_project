---
marp: true
theme: default
paginate: true
math: mathjax
style: |
  section {
    font-family: "DejaVu Sans", "Liberation Sans", sans-serif;
    font-size: 24px;
    line-height: 1.35;
    color: #1a202c;
    background: #ffffff;
    padding: 40px 52px;
  }
  h1 {
    font-size: 36px;
    color: #1a2b4c;
    margin-bottom: 12px;
  }
  h2 {
    font-size: 28px;
    color: #1a2b4c;
    padding-bottom: 6px;
    margin-bottom: 18px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 20px;
  }
  th, td {
    border: 1px solid #cbd5e1;
    padding: 8px 12px;
    text-align: left;
  }
  th {
    background: #f1f5f9;
    color: #1a2b4c;
  }
  .muted {
    color: #475569;
    font-size: 18px;
    margin-top: 12px;
  }
  .cols {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 28px;
    align-items: start;
  }
  .final {
    text-align: center;
    font-size: 48px;
  }
---

# Аномалії тривалості поїздок NYC TLC відносно погодинної норми маршруту


Остапенко Дмитро, МП ІПЗ-1  

---

## Задача та дані

**Мета:** знайти зони `DOLocationID` із найбільшою часткою затримок (`ratio >= 1.5`) відносно медіани `(PU, DO, hour)`.

**Вхід:** `48 722 602` рядків (`~4.8 ГБ` CSV) -> `46 266 382` валідних поїздок (`-5.04%`).

---

## Схема етапів та ключі

<div class="cols">
<div>

- Етап 0: фільтр + проєкція (`pu_id, do_id, hour, dur_sec`).
- Shuffle 1 `(PU, DO, hour)`: медіана (`percentile_approx`) та `cnt >= 3`.
- `SortMergeJoin` + `ratio` (`is_delayed` при `>= 1.5`) -> Shuffle 2 `DOLocationID` (`259` зон).
- Перекіс по `DO`: топ-10 зон = `30.23%` поїздок (лідер `236`: `1.88 млн`, `4.05%`).

</div>

<div>

![Plan](./img/plan.jpg)

</div>
</div>

---

## Оцінка проти факту

| Метрика | Оцінка | Факт (Baseline) | Причина |
| :--- | :--- | :--- | :--- |
| Прочитано | ~4.8 ГБ (1 раз) | `23.72 GiB` | 2 скани CSV + `show(20)` і `write.csv` |
| Партиції | Базова оцінка | `16` (AQE) | Динамічне згортання AQE |
| Spill | — | `4.27 GiB` (`27`) | Стан `percentile_approx` + `2.2 GiB` BlockManager |

- Найважчі таски (Stages 6, 15): до `17.1 с` та `14.8 с`, spill до `216.1 MiB`.

---

## Експеримент: `.persist(MEMORY_AND_DISK)`

- Зміна: кеш очищеного `trips_df` перед медіаною та `join`.
- Гіпотеза: час `-20..35%` (`110–135 с`), читання CSV удвічі менше.

| Варіант (медіана 3 запусків) | Час (с) | Прочитано | Shuffle | Spill |
| :--- | :--- | :--- | :--- | :--- |
| Baseline | `181.1` | `23.72 GiB` | `1.77 GiB` | `4.27 GiB` (`27`) |
| `.persist(MEMORY_AND_DISK)` | `133.6` (`-26.2%`) | `10.87 GiB` (`-54.2%`) | `1.77 GiB` | `4.77 GiB` (`50`) |

- Читання і час впали; spill зріс через менший Execution pool у `2.2 GiB`.

---

## Результати по зонах (`DOLocationID`)

<div class="cols">
<div>


| `DO_ID` | Поїздок | `delay_rate` | `avg_ratio` |
| :--- | :--- | :--- | :--- |
| **264** | `89 963` | **26.18%** | `1.230` |
| **265** | `198 394` | **21.90%** | `1.150` |
| **43** | `452 336` | **16.22%** | `1.081` |
| **207** | `647` | **15.77%** | `1.340` |
| **237** | `1 859 788` | **14.00%** | `1.080` |

</div>

<div>

![](./img/results.png)

</div>
</div>

---

<h1 class="final">Демонстрація та питання</h1>
