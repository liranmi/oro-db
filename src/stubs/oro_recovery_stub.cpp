/*
 * oro-db stub for mtls_recovery_manager.cpp
 * Provides just enough to satisfy the vtable.
 */
#include "mtls_recovery_manager.h"

namespace MOT {

MTLSRecoveryManager::~MTLSRecoveryManager() {}
bool MTLSRecoveryManager::Initialize() { return true; }
bool MTLSRecoveryManager::RecoverDbStart() { return false; }
bool MTLSRecoveryManager::RecoverDbEnd() { return false; }
bool MTLSRecoveryManager::CommitTransaction(uint64_t) { return false; }
bool MTLSRecoveryManager::DeserializePendingRecoveryData(int) { return false; }
bool MTLSRecoveryManager::ApplyLogSegmentData(char*, size_t, uint64_t) { return false; }
uint64_t MTLSRecoveryManager::SerializePendingRecoveryData(int) { return 0; }
void MTLSRecoveryManager::Flush() {}

}  // namespace MOT
