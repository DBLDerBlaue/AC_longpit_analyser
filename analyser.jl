using CSV
using DataFrames

data = CSV.read("logs/cota_6h_remote.csv", DataFrame)
cl_laps_data = CSV.read("logs/cota_6h_clean_laptime.csv", DataFrame)

cl_laps = Dict(strip.(cl_laps_data[:, 2]) .=> time_to_seconds.(strip.(cl_laps_data[:, 8])))
total = Dict(strip.(cl_laps_data[:, 2]) .=> time_to_seconds_hours.(strip.(string.(cl_laps_data[:, 6]))))

drivers = strip.(cl_laps_data[:, 2])
#drivers = data[18:55, 5]

function time_to_seconds(s::T) where T <: AbstractString
    return sum([60, 1] .* parse.(Float64, split(s, ":"))[end-1:end])
end

function time_to_seconds(s::Missing)
    return 0.0
end

function time_to_seconds_hours(s)
    parse(Float64, split(s, ":")[1])*60^2+time_to_seconds(s[4:end])
end

function get_driver_laptimes(driver, data)
    id = data[:, 1]
    idx = 0
    idx_end = 0
    for (i, el) in enumerate(id)
        if el == driver
            idx = i
            break
        end
    end
    if idx == 0
        return nothing
    end
    for i in idx:length(id)
        if id[i] == "Average"
            idx_end = i-1
            break
        end
    end
    return time_to_seconds.(data[idx+1:idx_end, 5])
end

function total_longstop_time(driver, data, cl_laps, total; filter_time = 160)
    laptimes = get_driver_laptimes(driver, data)
    overtime = total[driver]-sum(laptimes)
    long_stops = filter(x -> x > filter_time, laptimes[2:end]) .- cl_laps[driver]
    return overtime + sum(long_stops)
end

function all_total_longstop_times(drivers, data, cl_laps, total; filter_time = 160)
    times = Dict{String, Float64}()
    for d in drivers
        times[d] = total_longstop_time(d, data, cl_laps, total, filter_time = filter_time)
    end
    return times
end

function all_finisher_total_longstop_times(drivers, data, cl_laps, total; filter_time = 160, race_time_hour = 6)
    times = all_total_longstop_times(drivers, data, cl_laps, total, filter_time = filter_time)
    for d in drivers
        if total[d] < race_time_hour*60*60
            pop!(times, d)
        end
    end
    return times
end

function all_finisher_longstops_valid(drivers, data, cl_laps, total; filter_time = 160, race_time_hour = 6, long_stop_time_minutes = 3, long_stop_amount = 4)
    times = all_finisher_total_longstop_times(drivers, data, cl_laps, total, filter_time = filter_time, race_time_hour = race_time_hour)
    valid = Dict{String, Bool}()
    for d in drivers
        try
            valid[d] = times[d] >= long_stop_time_minutes*60*long_stop_amount
        catch e

        end
    end
    return valid
end