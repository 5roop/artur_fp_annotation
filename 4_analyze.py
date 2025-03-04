from scipy.stats import ttest_ind, wilcoxon, ranksums
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(530)
pl.Config.set_tbl_width_chars(300)
pl.Config.set_fmt_str_lengths(20)


def calculate_fp_cummulative_durations(l: list) -> float:
    s = 0.0
    for i in l:
        s += i[1] - i[0]
    return s


def calculate_fp_individual_durations(l: list) -> list[float]:
    s = []
    for i in l:
        s.append(i[1] - i[0])
    return s


df = (
    pl.read_ndjson("3_inferred_FP.jsonl")
    .with_columns(
        pl.col("ident").str.split("-").list.slice(0, 2).list.join("-").alias("type"),
        pl.col("filled_pauses")
        .map_elements(calculate_fp_cummulative_durations, return_dtype=pl.Float64)
        .alias("FP_duration"),
        pl.col("filled_pauses")
        .map_elements(
            calculate_fp_individual_durations, return_dtype=pl.List(pl.Float64)
        )
        .alias("FP_durations"),
    )
    .filter(pl.col("who").is_not_null())
    .group_by("who")
    .agg(
        pl.col("ident").n_unique().alias("utterances_per_speaker"),
        pl.col("w_count").sum(),
        pl.col("FP_count").sum(),
        pl.col("FP_duration").sum(),
        pl.col("duration").sum(),
        pl.col("SEX").unique().first(),
        pl.col("type").unique().first(),
        pl.col("FP_durations").flatten().median().alias("median_FP_durations"),
        pl.col("FP_durations")
        .flatten()
        .quantile(0.9)
        .alias("90th_percentile_FP_durations"),
    )
    .with_columns(
        (100 * pl.col("FP_count") / pl.col("w_count")).alias("FP_percent"),
        (60 * pl.col("FP_count") / pl.col("duration")).alias("FP_per_minute"),
        (pl.col("FP_duration") / pl.col("duration")).alias("FP_duratio"),
        (pl.col("w_count") / pl.col("duration")).alias("wps"),
    )
    .filter(pl.col("w_count") >= 1000)
)

g = sns.displot(
    df,
    x="utterances_per_speaker",
    kind="hist",
    hue="type",
    multiple="dodge",
    bins=10,
    binrange=[0, 100],
)
g.savefig("images/utterances_distribution.png")
g = sns.displot(
    df, x="w_count", kind="hist", hue="type", multiple="dodge", binrange=[0, 6000]
)
g.savefig("images/w_count_distribution.png")

metric = "FP_per_minute"
# for metric in "FP_per_minute FP_duratio FP_percent".split():
for metric in ["FP_percent"]:
    print(
        df.group_by("SEX")
        .agg(
            pl.col("who").count().alias("nr_speakers"),
            pl.col(f"{metric}").median().round(3).alias(f"median_{metric}"),
        )
        .sort("SEX"),
        "\n",
        df.group_by(["type", "SEX"])
        .agg(
            pl.col("who").count().alias("nr_speakers"),
            pl.col(f"{metric}").median().round(3).alias(f"median_{metric}"),
        )
        .sort(["type", "SEX"]),
    )
    print("Metric:", metric)
    male = df.filter(pl.col("SEX").eq("moški"))[f"{metric}"].to_list()
    female = df.filter(pl.col("SEX").eq("ženski"))[f"{metric}"].to_list()
    res = ranksums(male, female, alternative="less")
    t_stat, p_value = res.statistic, res.pvalue
    print(f"P value  ranksums (alternative: male < female): {p_value:0.3f}")
    res = ttest_ind(male, female, alternative="less", equal_var=False)
    t_stat, p_value = res.statistic, res.pvalue
    print(f"P value  ttest (alternative: male < female): {p_value:0.3f}")
    for type in df["type"].unique().sort():
        print(
            f"Within type: {type}",
        )
        male = df.filter(pl.col("type").eq(type) & pl.col("SEX").eq("moški"))[
            f"{metric}"
        ].to_list()
        female = df.filter(pl.col("type").eq(type) & pl.col("SEX").eq("ženski"))[
            f"{metric}"
        ].to_list()
        res = ranksums(male, female, alternative="less")
        t_stat, p_value = res.statistic, res.pvalue
        print(f"P value  ranksums (alternative: male < female): {p_value:0.3f}")
        res = ttest_ind(male, female, alternative="less", equal_var=False)
        t_stat, p_value = res.statistic, res.pvalue
        print(f"P value  ttest (alternative: male < female): {p_value:0.3f}")

    sns.catplot(df, x="type", y=f"{metric}", kind="box")
    plt.gcf().savefig(f"images/box__{metric}__type.png")
    sns.catplot(df, x="type", y=f"{metric}", hue="SEX", kind="box")
    plt.gcf().savefig(f"images/box__{metric}__type_sex.png")
    sns.catplot(df, y=f"{metric}", x="SEX", kind="box")
    plt.gcf().savefig(f"images/box__{metric}__sex.png")

df = (
    df.select(
        [
            "median_FP_durations",
            "90th_percentile_FP_durations",
            "wps",
            "w_count",
            "duration",
            "type",
            "SEX",
        ]
    ).drop_nulls()
    # .filter(pl.col("type").eq("Artur-N"))
)

g = sns.PairGrid(
    data=df,
    hue="type",
)


def corrfunc(x, y, hue=None, ax=None, **kws):
    from scipy.stats import pearsonr

    # c = x.isnull() & y.isnull()
    # x = x[~c]
    # y = y[~c]
    """Plot the correlation coefficient in the top left hand corner of a plot."""
    # print(x, y)
    r, p = pearsonr(x, y)

    ax = ax or plt.gca()
    ax.annotate(rf"$\rho$ = {r:.3f}, {p=:0.2g}", xy=(0.1, 0.05), xycoords=ax.transAxes)


g.map_diag(sns.histplot, multiple="dodge")
g.map_offdiag(sns.scatterplot, alpha=0.3, style="SEX", data=df)
g.map_lower(corrfunc)
g.add_legend()
g.tight_layout()
g.savefig("images/speech_metrics.png")
2 + 2
