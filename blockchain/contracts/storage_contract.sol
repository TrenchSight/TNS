// SPDX-License-Identifier: MIT
pragma solidity ^0.8.17;

/**
 * @title StorageContract
 * @dev A contract to manage decentralized storage references for TrenchSight.
 *      It allows authorized roles to add, update, and remove storage entries.
 *      Entries can reference IPFS hashes or Arweave transaction IDs.
 */

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

contract StorageContract is AccessControl, Pausable {
    using Counters for Counters.Counter;

    // Define roles
    bytes32 public constant ADMIN_ROLE = keccak256("ADMIN_ROLE");
    bytes32 public constant UPDATER_ROLE = keccak256("UPDATER_ROLE");

    // Counter for generating unique IDs
    Counters.Counter private _storageIdCounter;

    // Enum for storage backend types
    enum StorageType { IPFS, Arweave }

    // Struct to store storage entry details
    struct StorageEntry {
        uint256 id;
        address owner;
        StorageType storageType;
        string identifier; // IPFS hash or Arweave tx ID
        uint256 timestamp;
    }

    // Mapping from storage ID to StorageEntry
    mapping(uint256 => StorageEntry) private _storageEntries;

    // Events
    event StorageAdded(uint256 indexed id, address indexed owner, StorageType storageType, string identifier, uint256 timestamp);
    event StorageUpdated(uint256 indexed id, StorageType storageType, string identifier, uint256 timestamp);
    event StorageRemoved(uint256 indexed id, uint256 timestamp);
    event OwnershipTransferred(uint256 indexed id, address indexed previousOwner, address indexed newOwner);

    /**
     * @dev Modifier to check if the caller is the owner of the storage entry.
     * @param id The ID of the storage entry.
     */
    modifier onlyEntryOwner(uint256 id) {
        require(_storageEntries[id].owner == msg.sender, "Caller is not the owner");
        _;
    }

    /**
     * @dev Constructor sets up roles and initializes the counter.
     */
    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _setupRole(ADMIN_ROLE, msg.sender);
        _storageIdCounter.increment(); // Start IDs from 1
    }

    /**
     * @dev Adds a new storage entry.
     * @param storageType The type of storage backend (IPFS or Arweave).
     * @param identifier The IPFS hash or Arweave transaction ID.
     * @return The ID of the newly added storage entry.
     */
    function addStorageEntry(StorageType storageType, string memory identifier) external whenNotPaused returns (uint256) {
        require(hasRole(ADMIN_ROLE, msg.sender) || hasRole(UPDATER_ROLE, msg.sender), "Caller is not authorized to add storage entries");
        require(bytes(identifier).length > 0, "Identifier cannot be empty");

        uint256 currentId = _storageIdCounter.current();
        _storageEntries[currentId] = StorageEntry({
            id: currentId,
            owner: msg.sender,
            storageType: storageType,
            identifier: identifier,
            timestamp: block.timestamp
        });

        emit StorageAdded(currentId, msg.sender, storageType, identifier, block.timestamp);

        _storageIdCounter.increment();
        return currentId;
    }

    /**
     * @dev Updates an existing storage entry.
     * @param id The ID of the storage entry to update.
     * @param storageType The new storage backend type.
     * @param identifier The new IPFS hash or Arweave transaction ID.
     */
    function updateStorageEntry(uint256 id, StorageType storageType, string memory identifier) external whenNotPaused onlyEntryOwner(id) {
        require(bytes(identifier).length > 0, "Identifier cannot be empty");

        StorageEntry storage entry = _storageEntries[id];
        entry.storageType = storageType;
        entry.identifier = identifier;
        entry.timestamp = block.timestamp;

        emit StorageUpdated(id, storageType, identifier, block.timestamp);
    }

    /**
     * @dev Removes a storage entry.
     * @param id The ID of the storage entry to remove.
     */
    function removeStorageEntry(uint256 id) external whenNotPaused onlyEntryOwner(id) {
        delete _storageEntries[id];
        emit StorageRemoved(id, block.timestamp);
    }

    /**
     * @dev Transfers ownership of a storage entry to a new owner.
     * @param id The ID of the storage entry.
     * @param newOwner The address of the new owner.
     */
    function transferOwnership(uint256 id, address newOwner) external whenNotPaused onlyEntryOwner(id) {
        require(newOwner != address(0), "New owner cannot be the zero address");

        address previousOwner = _storageEntries[id].owner;
        _storageEntries[id].owner = newOwner;

        emit OwnershipTransferred(id, previousOwner, newOwner);
    }

    /**
     * @dev Retrieves a storage entry by ID.
     * @param id The ID of the storage entry.
     * @return The StorageEntry struct.
     */
    function getStorageEntry(uint256 id) external view returns (StorageEntry memory) {
        require(_storageEntries[id].id != 0, "Storage entry does not exist");
        return _storageEntries[id];
    }

    /**
     * @dev Retrieves all storage entries owned by an address.
     * @param owner The address to query.
     * @return An array of StorageEntry structs.
     */
    function getStorageEntriesByOwner(address owner) external view returns (StorageEntry[] memory) {
        uint256 totalEntries = _storageIdCounter.current() - 1;
        uint256 count = 0;

        // First pass to count entries
        for (uint256 i = 1; i <= totalEntries; i++) {
            if (_storageEntries[i].owner == owner) {
                count++;
            }
        }

        // Second pass to collect entries
        StorageEntry[] memory entries = new StorageEntry[](count);
        uint256 index = 0;
        for (uint256 i = 1; i <= totalEntries; i++) {
            if (_storageEntries[i].owner == owner) {
                entries[index] = _storageEntries[i];
                index++;
            }
        }

        return entries;
    }

    /**
     * @dev Pauses the contract. Only callable by ADMIN_ROLE.
     */
    function pause() external onlyRole(ADMIN_ROLE) {
        _pause();
        logger.info("Contract paused by %s", msg.sender);
    }

    /**
     * @dev Unpauses the contract. Only callable by ADMIN_ROLE.
     */
    function unpause() external onlyRole(ADMIN_ROLE) {
        _unpause();
        logger.info("Contract unpaused by %s", msg.sender);
    }

    /**
     * @dev Grants UPDATER_ROLE to an account. Only callable by ADMIN_ROLE.
     * @param account The address to grant the role.
     */
    function grantUpdaterRole(address account) external onlyRole(ADMIN_ROLE) {
        grantRole(UPDATER_ROLE, account);
        logger.info("Granted UPDATER_ROLE to %s", account);
    }

    /**
     * @dev Revokes UPDATER_ROLE from an account. Only callable by ADMIN_ROLE.
     * @param account The address to revoke the role.
     */
    function revokeUpdaterRole(address account) external onlyRole(ADMIN_ROLE) {
        revokeRole(UPDATER_ROLE, account);
        logger.info("Revoked UPDATER_ROLE from %s", account);
    }
}

