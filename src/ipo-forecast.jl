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
        @transform(Year = Dates.year(:TradeDate))
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
