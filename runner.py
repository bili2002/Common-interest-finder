import sys
import polars as pl

ALL_INTERESTS_DF = "df_base"
CSV_DIR = "csv_files/"
EXTENSION = ".csv"
INTERESTS = "interests"
PRIORITY = "priority" #0 - don't want, 1 - would want if the other wants, 2 - I want


def readDF(name):
    df = pl.read_csv(CSV_DIR + name + EXTENSION)
    return df


def writeDF(df, name):
    df.write_csv(CSV_DIR + name + EXTENSION)


def commonInterests(df, df_other):
    df_other = df_other.filter(pl.col(PRIORITY) > 0)
    df_new = df.join(df_other, on=INTERESTS, how="inner", suffix="other")
    df_new = df_new.select(
        pl.col(INTERESTS), (pl.col(PRIORITY) + pl.col(PRIORITY + "other")
    ).alias(PRIORITY))

    return df_new


def findAllCommonInterests(beg):
    df_main = readDF(sys.argv[beg]).filter(pl.col(PRIORITY) > 0)

    for i in range(beg + 1, len(sys.argv)):
        df_temp = readDF(sys.argv[i])
        df_main = commonInterests(df_main, df_temp)

    majority = (len(sys.argv) - beg) * 3 // 2
    all_common = df_main.filter(pl.col(PRIORITY) >= majority).sort(PRIORITY, descending=True)[INTERESTS].to_list()
    printAnswer(all_common)

def printAnswer(all_common):
    print("\nCommon interests:")
    for i, interest in enumerate(all_common):
        print(f"{i + 1}. {interest}")
    print()


def addInterests(beg):
    intrests_list = sys.argv[beg : len(sys.argv)]

    df_base = readDF(ALL_INTERESTS_DF)
    df_new = pl.DataFrame({
        INTERESTS: intrests_list
    })

    df_base = pl.concat([df_base, df_new])
    writeDF(df_base, ALL_INTERESTS_DF)


def rateInterests(person):
    intrests_list = readDF(ALL_INTERESTS_DF)[INTERESTS].to_list()
    priority_list = [
        int(input(f"Rate {interest} 0-2: "))
        for interest in intrests_list
    ]
    
    df_new = pl.DataFrame({
        INTERESTS: intrests_list,
        PRIORITY: priority_list
    })

    writeDF(df_new, person)


def allRateInterests(beg):
    for person in (sys.argv[beg :]):
        print(f"Now {person} should rate!")
        rateInterests(person)


def makeProject():
    df_new = pl.DataFrame({
        INTERESTS: []
    })

    writeDF(df_new, ALL_INTERESTS_DF)


if __name__ == "__main__":
    if (len(sys.argv) < 2):
        raise "Not enough arguments"
    
    if (sys.argv[1] == "start"):
        makeProject()
    elif (len(sys.argv) < 3):
        raise "Not enough arguments"
    
    if (sys.argv[1] == "add"):
        addInterests(2)
    elif (sys.argv[1] == "rate"):
        allRateInterests(2)
    elif (sys.argv[1] == "find"):
        findAllCommonInterests(2)
    elif (sys.argv[1] == "all"):
        allRateInterests(2)
        findAllCommonInterests(2)


# add delete interests
# add delete all users info