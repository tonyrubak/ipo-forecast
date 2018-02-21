using CSV
using DataFrames
using DataFramesMeta
using Lazy
using Plots
using StatPlots
using GLM

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

get_day_close = function(data, date)
    close = -1
    if (Dates.dayname(date) == "Saturday")
        date -= Dates.Day(1)
    elseif (Dates.dayname(date) == "Sunday")
        date -= Dates.Day(2)
    end
    while (close == -1)
        tclose = @> begin
            data
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
    
get_week_change = function(data, date)
    chg = 0
    try
        day_ago = get_day_close(data, date - Dates.Day(1))
        week_ago = get_day_close(data, date - Dates.Day(8))
        chg = (day_ago - week_ago) / week_ago * 100
    catch
        println("error in week $date")
    end
    chg
end

get_cto_change = function(data, date)
    try
        today_open = (@> begin
                      data
                      @where(:Date .== date)
                      @select(:Open)
                      end)[1,1]
        yday_close = get_day_close(data, date - Dates.Day(1))
        (today_open - yday_close) / yday_close * 100
    catch
        println("error in cto $date")
    end
end

metric_profit = function(data)
    data = data[data[:pred] .== 1,:]
    data[:Close] - data[:Opening]
end

metric_error = function(data)
    (data[:Y] - data[:pred]) .^ 2
end

kfold_cross_validate = function(data, model, k, prediction_rule, metric)
    scores = []
    i = 1
    rows = size(data)[1]

    # Divide the data into k folds
    cut_len = Int64(floor(rows / k))
    cuts = [0; map(x -> x * cut_len, 1:k-1); rows]
    folds = map(x -> data[cuts[x]+1:cuts[x+1],:], 1:k)

    # Perform validation
    while (i <= k)
        train = vcat(deleteat!(copy(folds), i)...)
        test = folds[i]
        m = model(train)
        preds = predict(m, test)
        pred_class = map(x -> prediction_rule(x), preds)
        test = @transform(test, pred .= pred_class)
        score = sum(metric.(test))
        push!(scores, score)
        i += 1
    end
    println(scores)
    return mean(scores)
end

main = function()
    df = clean_data(CSV.read("data/ipo-data.csv"))
    spy = CSV.read("data/spy.csv")
    # Exploratory analysis
    
    # Look at the average performance on the first day for each year
    # @df @based_on(DataFrames.groupby(df, :Year), means = mean(:FirstDay)) plot(:Year, :means, linetype = :bar)
    
    # @df @based_on(DataFrames.groupby(df, :Year), medians = median(:FirstDay)) plot(:Year, :medians, linetype = :bar)

    # Summary statistics of first day change
    #    describe(df[:FirstDay])
    #    histogram(df[:FirstDay])
    
    # Add columns for change open to close and examine the data
    df = @transform(df,
                    DollarOpenClose = :Close - :Opening,
                    PerOpenClose = broadcast((x, y) -> x / y, (:Close - :Opening), :Opening) * 100)
    # describe(df[:DollarOpenClose])
    # describe(df[:PerOpenClose])

    # Feature Engineering

    # Delete trades with erroneous dates (those with a trade date on the weekend) and add:
    # (1) prior week change S&P 500
    # (2) prior-day close to offer-day open change S&P 500
    # (3) ratio of $ change at open to opening price
    
    weekends = Set(["Saturday","Sunday"])

    df = @> begin
        df
        @where(map(x -> ~(Dates.dayname(x) in weekends), :TradeDate))
        @transform(WeekChg = map(x -> get_week_change(spy, x), :TradeDate),
                   CTOChg = map(x -> get_cto_change(spy, x), :TradeDate),
                   GapOpenPct = :ChangeOpen ./ :Opening * 100,
                   OpenClosePct = (:ChangeClose .- :ChangeOpen) ./ :ChangeOpen * 100)
    end
    
    # Classification: Use 2017+ for testing, rest of data for training
    # A success is determined as $ change open to close of greater than $1

    train = @> begin
        df
        @where(:Year .< 2017)
        @transform(Y = map(x -> x .> 1 ? 1 : 0, :DollarOpenClose))
        @select(:GapOpenPct, :ChangeOpen, :Offer, :Opening, :CTOChg, :WeekChg, :Close, :Y)
    end
    
    test = @> begin
        df
        @where(:Year .>= 2017)
        @transform(Y = map(x -> x .> 1 ? 1 : 0, :DollarOpenClose))
        @select(:GapOpenPct, :ChangeOpen, :Offer, :Opening, :CTOChg, :WeekChg, :Close, :Y)
    end

    # Attempt to find an accurate parameter for class detection

    param = 0.
    results = []

    metric = metric_profit
    
    while (param <= 1.)
        res = kfold_cross_validate(train, (data -> glm(@formula(Y ~ GapOpenPct + ChangeOpen + Offer + Opening + CTOChg + WeekChg), data, Binomial(), LogitLink())), 4, (x -> x >= param ? 1 : 0), metric)
        push!(results, res)
        param += 0.05
    end
    pred_param = 0.05 * indmax(results)

    # Run model on test data with discovered prediction parameter
    m0 = glm(@formula(Y ~ GapOpenPct + ChangeOpen + Offer + Opening + CTOChg + WeekChg), train, Binomial(), LogitLink())
    test_pred = predict(m0, test)
    test_classes = map(x -> x >= pred_param ? 1 : 0, test_pred)
    println(pred_param)

    test_classes
end
