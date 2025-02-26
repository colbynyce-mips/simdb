// <ConcurrentPriorityQueue.hpp> -*- C++ -*-

#pragma once

#include <algorithm>
#include <mutex>
#include <queue>
#include <vector>

namespace simdb
{

template <typename T, typename Compare = std::less<T>> class ConcurrentPriorityQueue
{
public:
    void push(const T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        pqueue_.push(item);
    }

    template <typename... Args> void emplace(Args&&... args)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        pqueue_.emplace(std::forward<Args>(args)...);
    }

    bool try_pop(T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (pqueue_.empty())
        {
            return false;
        }
        item = pqueue_.top();
        pqueue_.pop();
        return true;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return pqueue_.size();
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return pqueue_.empty();
    }

private:
    mutable std::mutex mutex_;
    std::priority_queue<T, std::vector<T>, Compare> pqueue_;
};

} // namespace simdb
