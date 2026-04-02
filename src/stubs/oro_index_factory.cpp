/*
 * oro-db replacement for index_factory.cpp
 * Uses MasstreePrimaryIndex when ORO_HAS_MASSTREE is defined,
 * falls back to StubPrimaryIndex (std::map-based) otherwise.
 */
#include "index_factory.h"
#include "oro_stub_index.h"
#ifdef ORO_HAS_MASSTREE
#include "masstree_index.h"
#endif
#include "utilities.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(IndexFactory, Storage);

Index* IndexFactory::CreateIndex(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor)
{
    Index* newIx = CreatePrimaryIndex(indexingMethod, flavor);
    if (newIx != nullptr) {
        newIx->SetOrder(indexOrder);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to create primary index");
    }
    return newIx;
}

Index* IndexFactory::CreateIndexEx(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor,
    bool unique, uint32_t keyLength, const std::string& name, RC& rc, void** args)
{
    Index* index = IndexFactory::CreateIndex(indexOrder, indexingMethod, flavor);
    if (index == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to create index");
        rc = RC_ABORT;
    } else {
        rc = index->IndexInit(keyLength, unique, name, args);
        if (rc != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to initialize index: %s", RcToString(rc));
            delete index;
            index = nullptr;
        }
    }
    return index;
}

Index* IndexFactory::CreatePrimaryIndex(IndexingMethod indexingMethod, IndexTreeFlavor flavor)
{
    Index* result = nullptr;
    switch (indexingMethod) {
        case IndexingMethod::INDEXING_METHOD_TREE:
            result = CreatePrimaryTreeIndex(flavor);
            break;
        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Create Primary Index",
                "Cannot create primary index: invalid indexing method %d",
                (int)indexingMethod);
            break;
    }
    return result;
}

Index* IndexFactory::CreatePrimaryIndexEx(IndexingMethod indexingMethod, IndexTreeFlavor flavor, uint32_t keyLength,
    const std::string& name, RC& rc, void** args)
{
    return IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY, indexingMethod, flavor, true, keyLength, name, rc, args);
}

Index* IndexFactory::CreatePrimaryTreeIndex(IndexTreeFlavor flavor)
{
    Index* result = nullptr;
    switch (flavor) {
        case IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE:
#ifdef ORO_HAS_MASSTREE
            MOT_LOG_DEBUG("Creating MasstreePrimaryIndex.");
            result = new (std::nothrow) MasstreePrimaryIndex();
#else
            MOT_LOG_DEBUG("Creating StubPrimaryIndex (masstree not linked).");
            result = new (std::nothrow) StubPrimaryIndex();
#endif
            break;
        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Create Primary Tree Index",
                "Cannot create primary tree index: invalid flavor %u",
                flavor);
            return nullptr;
    }
    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Primary Tree Index",
            "Failed to allocate primary index: out of memory");
    }
    return result;
}
}  // namespace MOT
