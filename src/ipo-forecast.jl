using CSV
using DataFrames
using DataFramesMeta
using Lazy
using Plots
using StatPlots

strip_punctuation = function(s)
    """
    strip_punctuation(s)

Takes a string and removes \$ and \%, and replaces () with - for negative numbers.
    """
    @> begin
        s
        replace(r"[$%\)]","")
        replace(r"\(","-")
    end
end

clean_data = function(df)
    """
    clean_data(df)

Convert the TradeDate column to a date and convert the price columns from strings to numbers.
Adds a year column.
    """
    @> begin
        df
        @transform(TradeDate = broadcast(x -> Dates.Date(x, "m/dd/yyyy"), :TradeDate),
                   Offer = broadcast(x -> parse(Float64,  strip_punctuation(x)), :Offer),
                   Opening = broadcast(x -> parse(Float64,  strip_punctuation(x)), :Opening),
                   Close = broadcast(x -> parse(Float64,  strip_punctuation(x)), :Close),
                   FirstDay = broadcast(x -> parse(Float64,  strip_punctuation(x)), :FirstDay),
                   ChangeOpen = broadcast(x -> parse(Float64,  strip_punctuation(x)), :ChangeOpen),
                   ChangeClose = broadcast(x -> parse(Float64,  strip_punctuation(x)), :ChangeClose))
        @transform(Year = Dates.year.(:TradeDate))
    end
end

# Exploratory analysis

df = clean_data(CSV.read("data/ipo-data.csv"))

# Look at the average performance on the first day for each year
@df @based_on(DataFrames.groupby(df, :Year), means = mean(:FirstDay)) plot(:Year, :means, linetype = :bar)

@df @based_on(DataFrames.groupby(df, :Year), medians = median(:FirstDay)) plot(:Year, :medians, linetype = :bar)

# Summary statistics of first day change
describe(df[:FirstDay])
histogram(df[:FirstDay])

# Add columns for change open to close and examine the data
df = @transform(df,
                DollarOpenClose = :Close - :Opening,
                PerOpenClose = broadcast((x, y) -> x / y, (:Close - :Opening), :Opening) * 100)
describe(df[:DollarOpenClose])
describe(df[:PerOpenClose])

# Feature Engineering
spy = CSV.read("data/spy.csv")

get_day_close = function(date)
    close = -1
    if (Dates.dayname(date) == "Saturday")
        date -= Dates.Day(1)
    elseif (Dates.dayname(date) == "Sunday")
        date -= Dates.Day(2)
    end
    while (close == -1)
        tclose = @> begin
            spy
            @where(:Date .== date)
            @select(:Close)
        end
        if (size(tclose)[1] > 0)
            close = tclose[1,1]
        elseif (Dates.dayname(date) == "Monday")
            date -= Dates.Day(2)
        end
        date -= Dates.Day(1)
    end
    close
end
    
get_week_change = function(date)
    chg = 0
    try
        day_ago = get_day_close(date - Dates.Day(1))
        week_ago = get_day_close(date - Dates.Day(8))
        chg = (day_ago - week_ago) / week_ago * 100
    catch
        println("error $date")
    end
    chg
end

get_cto_change = function(date)
    try
        today_open = (@> begin
                      spy
                      @where(:Date .== date)
                      @select(:Open)
                      end)[1,1]
        yday_close = get_day_close(date - Dates.Day(1))
        (today_open - yday_close) / yday_close * 100
    catch
        println("error $date")
    end
end

# Delete trades with erroneous dates (those with a trade date on the weeknde) and add:
# (1) prior week change S&P 500
# (2) prior-day close to offer-day open change S&P 500
# (3) ratio of $ change at open to opening price

weekends = Set(["Saturday","Sunday"])

df = @> begin
    df
    @where(map(x -> length(intersect(Set([Dates.dayname.(x)]), weekends)) .== 0, :TradeDate))
    @transform(WeekChg = map(x -> get_week_change(x), :TradeDate),
               CTOChg = map(x -> get_cto_change(x), :TradeDate),
               GapOpenPct = :ChangeOpen ./ :Opening * 100,
               OpenClosePct = (:ChangeClose .- :ChangeOpen) ./ :ChangeOpen * 100)
end

# Create the feature matrix
features = hcat(ones(df[:GapOpenPct]), df[:GapOpenPct], df[:ChangeOpen], df[:Offer], df[:Opening],
                df[:CTOChg], df[:WeekChg])
