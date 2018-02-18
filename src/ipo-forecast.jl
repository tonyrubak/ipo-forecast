using CSV
using DataFrames
using DataFramesMeta
using Lazy

strip_punctuation = function(s)
    """
Takes a string and removes $ and %, and replaces () with - for negative numbers.
    """
    @> begin
        s
        replace(r"[$%\)]","")
        replace(r"\(","-")
    end
end

df = CSV.read("data/ipo-data.csv")

