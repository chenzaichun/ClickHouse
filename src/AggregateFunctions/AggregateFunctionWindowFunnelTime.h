#pragma once

#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include "Functions/FunctionSnowflake.h"

#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr size_t max_events = 32;

template <typename T>
struct AggregateFunctionWindowFunnelTimeData
{
    using TimestampEvent = std::vector<T>;
    using TimestampEvents = std::map<UInt8, TimestampEvent>;
    using TimestampEventsSorted = std::map<UInt8, bool>;

    /// events map lists
    TimestampEvents events_list;

    /// events map lists which stored sorted information
    TimestampEventsSorted events_list_sorted;

    size_t size() const
    {
        return events_list.size();
    }

    void add(T timestamp, UInt8 event)
    {
        if (!events_list_sorted.contains(event)) {
            events_list_sorted[event] = true;
        }

        /// Since most events should have already been sorted by timestamp.
        if (events_list_sorted[event] && events_list[event].size() > 0)
        {
            events_list_sorted[event] = events_list[event].back() <= timestamp;
        }

        events_list[event].push_back(timestamp);
    }

    void merge(const AggregateFunctionWindowFunnelTimeData & other)
    {
        if (other.events_list.empty())
            return;

        for (auto & o : other.events_list) {
            if (o.second.empty())
                continue;

            UInt8 event = o.first;

            if (!events_list_sorted.contains(event))
            {
                events_list_sorted[event] = true;
            }

            TimestampEvent& event_list = events_list[event];
            std::copy(o.second.begin(), o.second.end(), std::back_inserter(event_list));
            events_list_sorted[event] = false;
        }
    }

    void sort()
    {
        for (auto& events_vector : events_list)
        {
            UInt8 event = events_vector.first;

            if (!events_list_sorted[event])
            {
                std::stable_sort(std::begin(events_vector.second),
                                 std::end(events_vector.second));
                events_list_sorted[event] = true;
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(events_list.size(), buf);
        bool sorted;
        for (auto events_vector : events_list) {
            UInt8 event = events_vector.first;
            writeBinary(event, buf);
            sorted = events_list_sorted.at(event);
            writeBinary(sorted, buf);
            writeBinary(events_vector.second.size(), buf);

            for (const auto & events : events_vector.second)
            {
                writeBinary(events, buf);
            }
        }
     }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readBinary(size, buf);

        /// TODO Protection against huge size

        events_list.clear();

        T timestamp;

        for (size_t i = 0; i < size; ++i)
        {
            size_t event_size;
            bool sorted;
            UInt8 event;
            readBinary(event, buf);
            readBinary(sorted, buf);
            readBinary(event_size, buf);

            events_list_sorted[event] = sorted;

            events_list[event].clear();
            events_list[event].reserve(event_size);

            for (size_t j = 0; j < event_size; ++j)
            {
                readBinary(timestamp, buf);
                events_list[event].push_back(event);
            }
        }
    }
};

/** Calculates the max event time delta in a sliding window.
  * The max size of events is 32, that's enough for funnel analytics
  *
  * Usage:
  * - windowFunnelTime(window)(timestamp, cond1, cond2, cond3, ....)
  */
template <typename T, typename Data>
class AggregateFunctionWindowFunnelTime final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnelTime<T, Data>>
{
private:
    UInt64 window;
    UInt8 events_size;
    /// When the 'strict_deduplication' is set, it applies conditions only for the not repeating values.
    bool strict_deduplication;

    /// When the 'strict_order' is set, it doesn't allow interventions of other events.
    /// In the case of 'A->B->D->C', it stops finding 'A->B->C' at the 'D' and the max event level is 2.
    bool strict_order;

    /// Applies conditions only to events with strictly increasing timestamps
    bool strict_increase;

    std::optional<T> findEventTimeStamp(std::vector<T> list, T first, T second) const
    {
        for (auto & element : list)
        {
            if (element >= first)
            {
                if (element <= second)
                    return element;
                else
                    return std::optional<T>{};
            }
        }

        return std::optional<T>{};
    }
    /// Loop through the entire events_list, update the event timestamp value
    /// The level path must be 1---2---3---...---check_events_size, find the max event level that satisfied the path in the sliding window.
    /// If found, returns the time delta if reach the max level, else return 0.
    /// The algorithm works in O(n) time, but the overall function works in O(n * log(n)) due to sorting.
    UInt64 getEventLevel(Data & data) const
    {
        if (data.size() <= 1 || data.size() != events_size)
            return 0;

        data.sort();

        /// interate first event
        for (auto & event0 : data.events_list[1])
        {
            /// interate last event
            for (auto & lastevent : data.events_list[events_size])
            {
                auto first_timestamp = event0;
                auto time_matched = lastevent <= event0 + window;
                auto last_timestamp = lastevent;
                if (time_matched)
                {
                    if (events_size == 2)
                    {
                        return last_timestamp - first_timestamp;
                    }

                    std::optional<T> t = std::optional<T>{first_timestamp};
                    for (auto event_idx = 1; event_idx < events_size; ++event_idx)
                    {
                        t = this->findEventTimeStamp(data.events_list[event_idx],
                                                     t.value(), lastevent);

                        if (!t.has_value())
                        {
                            break;
                        }

                        if (event_idx == (events_size - 1))
                        {
                            return last_timestamp - first_timestamp;
                        }
                    }
                }
                else
                {
                    break;
                }
            }
        }

        return 0;
    }

public:
    String getName() const override
    {
        return "windowFunnelTime";
    }

    AggregateFunctionWindowFunnelTime(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnelTime<T, Data>>(arguments, params)
    {
        events_size = arguments.size() - 1;
        window = params.at(0).safeGet<UInt64>();

        strict_deduplication = false;
        strict_order = false;
        strict_increase = false;
        for (size_t i = 1; i < params.size(); ++i)
        {
            String option = params.at(i).safeGet<String>();
            if (option == "strict_deduplication")
                strict_deduplication = true;
            else if (option == "strict_order")
                strict_order = true;
            else if (option == "strict_increase")
                strict_increase = true;
            else if (option == "strict")
                throw Exception{"strict is replaced with strict_deduplication in Aggregate function " + getName(), ErrorCodes::BAD_ARGUMENTS};
            else
                throw Exception{"Aggregate function " + getName() + " doesn't support a parameter: " + option, ErrorCodes::BAD_ARGUMENTS};
        }
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<AggregateFunctionNullVariadic<false, false, false>>(nested_function, arguments, params);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        bool has_event = false;
        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];
        /// reverse iteration and stable sorting are needed for events that are qualified by more than one condition.
        for (auto i = events_size; i > 0; --i)
        {
            auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                this->data(place).add(timestamp, i);
                has_event = true;
            }
        }

        if (strict_order && !has_event)
            this->data(place).add(timestamp, 0);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(getEventLevel(this->data(place)));
    }
};

}
