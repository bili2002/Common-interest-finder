import sys
import polars as pl

ALL_INTERESTS_DF = "df_base.csv"
INTERESTS = "interests"
PRIORITY = "priority" #0 - don't want, 1 - would want if the other wants, 2 - I want


def commonInterests(df, df_other):
    df_other = df_other.filter(pl.col(PRIORITY) > 0)
    df_new = df.join(df_other, on=INTERESTS, how="inner", suffix="other")
    df_new = df_new.select(
        pl.col(INTERESTS), (pl.col(PRIORITY) + pl.col(PRIORITY + "other")
    ).alias(PRIORITY))

    return df_new


def findAllCommonInterests(beg):
    df_main = pl.read_csv(sys.argv[beg]).filter(pl.col(PRIORITY) > 0)

    for i in range(beg + 1, len(sys.argv)):
        df_temp = pl.read_csv(sys.argv[i])
        df_main = commonInterests(df_main, df_temp)

    majority = (len(sys.argv) - beg) * 3 // 2
    df_main = df_main.filter(pl.col(PRIORITY) > majority).sort(PRIORITY).select(INTERESTS)


def addInterests(beg):
    intrests_list = sys.argv[beg + 1 : len(sys.argv)]

    df_base = pl.read_csv(ALL_INTERESTS_DF)
    df_new = pl.DataFrame({
        INTERESTS: intrests_list
    })

    df_base = pl.concat([df_base, df_new])
    df_base.write_csv(ALL_INTERESTS_DF)


def rateInterests(beg):
    intrests_list = pl.read_csv(ALL_INTERESTS_DF).select(INTERESTS)
    priority_list = [
        int(input(f"Rate {interest} out of 3"))
        for interest in intrests_list
    ]
    
    df_new = pl.DataFrame({
        INTERESTS: intrests_list,
        PRIORITY: priority_list
    })

    df_new.write_csv(sys.argv[beg])


def allRateInterests(beg):
    for i in range(beg + 1, len(sys.argv)):
        print(f"Now {sys.argv[i]} should rate!")
        rateInterests(i)


def makeProject():
    df_new = pl.DataFrame({
        INTERESTS: []
    })

    df_new.write_csv(ALL_INTERESTS_DF)


if __name__ == "__main__":
    if (len(sys.argv) < 2):
        raise "Not enough arguments"
    
    if (sys.argv[1] == "start"):
        makeProject()
    elif (sys.argv[1] == "rate"):
        allRateInterests(2)
    elif (sys.argv[1] == "rate1"):
        rateInterests(2)
    elif (sys.argv[1] == "add"):
        addInterests(2)
    elif (sys.argv[1] == "find"):
        findAllCommonInterests(2)
    
        