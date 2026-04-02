/*
 * oro-db stub index implementation
 *
 * A simple std::map-based index that implements the MOT Index interface.
 * This is a placeholder until the openGauss-patched MassTree library is integrated.
 * (Upstream kohler/masstree-beta has incompatible API with MOT's wrappers.)
 * It is NOT lock-free and NOT NUMA-aware — for build/test scaffolding only.
 */
#ifndef ORO_STUB_INDEX_H
#define ORO_STUB_INDEX_H

#include "index.h"
#include <map>
#include <mutex>
#include <cstring>

namespace MOT {

class StubIndexIterator : public IndexIterator {
public:
    StubIndexIterator()
        : IndexIterator(IteratorType::ITERATOR_TYPE_FORWARD, false, false), m_sentinel(nullptr)
    {}
    ~StubIndexIterator() override {}

    void Next() override { m_valid = false; }
    void Prev() override { m_valid = false; }
    bool Equals(const IndexIterator* rhs) const override { return this == rhs; }
    void Serialize(serialize_func_t, unsigned char*) const override {}
    void Deserialize(deserialize_func_t, unsigned char*) override {}
    const void* GetKey() const override { return nullptr; }
    Row* GetRow() const override { return nullptr; }
    Sentinel* GetPrimarySentinel() const override { return m_sentinel; }

    void Set(Sentinel* sentinel, bool valid)
    {
        m_sentinel = sentinel;
        m_valid = valid;
    }

private:
    Sentinel* m_sentinel;
};

/**
 * StubPrimaryIndex — simple ordered map index for build scaffolding.
 */
class StubPrimaryIndex : public Index {
public:
    StubPrimaryIndex()
        : Index(IndexOrder::INDEX_ORDER_PRIMARY, IndexingMethod::INDEXING_METHOD_TREE)
    {}

    ~StubPrimaryIndex() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_map.clear();
    }

    Sentinel* IndexInsertImpl(const Key* key, Sentinel* sentinel, bool& inserted, uint32_t pid) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string keyStr = MakeKeyString(key);
        auto it = m_map.find(keyStr);
        if (it != m_map.end()) {
            inserted = false;
            return static_cast<Sentinel*>(it->second);
        }
        m_map[keyStr] = sentinel;
        inserted = true;
        return nullptr;
    }

    Sentinel* IndexReadImpl(const Key* key, uint32_t pid) const override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string keyStr = MakeKeyString(key);
        auto it = m_map.find(keyStr);
        if (it != m_map.end()) {
            return static_cast<Sentinel*>(it->second);
        }
        return nullptr;
    }

    Sentinel* IndexRemoveImpl(const Key* key, uint32_t pid) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string keyStr = MakeKeyString(key);
        auto it = m_map.find(keyStr);
        if (it != m_map.end()) {
            Sentinel* s = static_cast<Sentinel*>(it->second);
            m_map.erase(it);
            return s;
        }
        return nullptr;
    }

    IndexIterator* Begin(uint32_t pid, bool passive = false) const override
    {
        m_iterA.Set(nullptr, false);
        return &m_iterA;
    }

    IndexIterator* Search(
        const Key* key, bool matchKey, bool forward, uint32_t pid, bool& found, bool passive = false) const override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string keyStr = MakeKeyString(key);
        auto it = m_map.find(keyStr);
        if (it != m_map.end()) {
            found = true;
            m_iterB.Set(static_cast<Sentinel*>(it->second), true);
        } else {
            found = false;
            m_iterB.Set(nullptr, false);
        }
        return &m_iterB;
    }

    uint64_t GetSize() const override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_map.size();
    }

    void Compact(Table* table, uint32_t pid) override {}

    RC ReInitIndex(bool isDummyIndex = false) override { return RC_OK; }

private:
    std::string MakeKeyString(const Key* key) const
    {
        if (key == nullptr) return {};
        const uint8_t* data = key->GetKeyBuf();
        uint32_t len = key->GetKeyLength();
        return std::string(reinterpret_cast<const char*>(data), len);
    }

    mutable std::mutex m_mutex;
    std::map<std::string, void*> m_map;
    mutable StubIndexIterator m_iterA;
    mutable StubIndexIterator m_iterB;
};

}  // namespace MOT
#endif /* ORO_STUB_INDEX_H */
