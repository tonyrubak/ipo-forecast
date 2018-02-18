using CSV
using DataFrames
using DataFramesMeta
using Lazy
using Requests

strip_punctuation = function(s) 
    @> begin
        s
        replace(r"[$%\)]","")
        replace(r"\(","-")
    end
end

df = CSV.read("data/ipo-data.csv")

