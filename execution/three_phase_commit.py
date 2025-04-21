"""
Three-Phase Commit Transaction System

This module implements a three-phase commit protocol for reliable order execution.
The protocol ensures that order-related operations are executed atomically and can
recover from failures.
"""

import logging
import time
import uuid
from enum import Enum
from typing import Dict, Any, Callable, Optional, List
import json
import os

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """
    Represents the various states of a transaction in the three-phase commit protocol.
    """
    INITIAL = "initial"
    PREPARING = "preparing" 
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"


class Transaction:
    """
    Represents a transaction in the three-phase commit protocol.
    """
    
    def __init__(self, transaction_id: Optional[str] = None, data: Dict[str, Any] = None):
        """
        Initialize a transaction.
        
        Args:
            transaction_id: Unique identifier for the transaction (auto-generated if None)
            data: Initial transaction data
        """
        self.transaction_id = transaction_id or str(uuid.uuid4())
        self.state = TransactionState.INITIAL
        self.data = data or {}
        self.timestamp = time.time()
        self.participants = []
        self.log_entries = []
    
    def add_participant(self, participant_id: str) -> None:
        """
        Add a participant to the transaction.
        
        Args:
            participant_id: Unique identifier for the participant
        """
        if participant_id not in self.participants:
            self.participants.append(participant_id)
    
    def update_state(self, new_state: TransactionState) -> None:
        """
        Update the transaction state and log the state change.
        
        Args:
            new_state: New transaction state
        """
        old_state = self.state
        self.state = new_state
        
        log_entry = {
            "timestamp": time.time(),
            "from_state": old_state.value,
            "to_state": new_state.value
        }
        self.log_entries.append(log_entry)
        
        logger.info(f"Transaction {self.transaction_id} state changed: {old_state.value} -> {new_state.value}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert transaction to dictionary.
        
        Returns:
            Transaction as dictionary
        """
        return {
            "transaction_id": self.transaction_id,
            "state": self.state.value,
            "data": self.data,
            "timestamp": self.timestamp,
            "participants": self.participants,
            "log_entries": self.log_entries
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transaction':
        """
        Create transaction from dictionary.
        
        Args:
            data: Transaction data dictionary
            
        Returns:
            Transaction instance
        """
        transaction = cls(transaction_id=data["transaction_id"], data=data["data"])
        transaction.state = TransactionState(data["state"])
        transaction.timestamp = data["timestamp"]
        transaction.participants = data["participants"]
        transaction.log_entries = data["log_entries"]
        return transaction
    
    def to_json(self) -> str:
        """
        Convert transaction to JSON string.
        
        Returns:
            Transaction as JSON string
        """
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Transaction':
        """
        Create transaction from JSON string.
        
        Args:
            json_str: JSON string
            
        Returns:
            Transaction instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)


class TransactionManager:
    """
    Manages transactions using the three-phase commit protocol.
    """
    
    def __init__(self, log_dir: str = "logs/transactions"):
        """
        Initialize transaction manager.
        
        Args:
            log_dir: Directory for transaction logs
        """
        self.transactions: Dict[str, Transaction] = {}
        self.log_dir = log_dir
        
        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Recover any pending transactions from logs
        self._recover_transactions()
    
    def create_transaction(self, data: Dict[str, Any] = None) -> Transaction:
        """
        Create a new transaction.
        
        Args:
            data: Initial transaction data
            
        Returns:
            New transaction
        """
        transaction = Transaction(data=data)
        self.transactions[transaction.transaction_id] = transaction
        self._save_transaction(transaction)
        return transaction
    
    def prepare(self, transaction_id: str, prepare_functions: Dict[str, Callable]) -> bool:
        """
        Execute the prepare phase of the three-phase commit protocol.
        
        Args:
            transaction_id: Transaction ID
            prepare_functions: Dict mapping participant IDs to their prepare functions
            
        Returns:
            True if all participants successfully prepared, False otherwise
        """
        if transaction_id not in self.transactions:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        transaction = self.transactions[transaction_id]
        transaction.update_state(TransactionState.PREPARING)
        self._save_transaction(transaction)
        
        # Execute prepare functions for all participants
        all_prepared = True
        for participant_id, prepare_func in prepare_functions.items():
            transaction.add_participant(participant_id)
            
            try:
                result = prepare_func(transaction)
                if not result:
                    logger.error(f"Participant {participant_id} failed to prepare for transaction {transaction_id}")
                    all_prepared = False
                    break
            except Exception as e:
                logger.exception(f"Error preparing participant {participant_id} for transaction {transaction_id}: {e}")
                all_prepared = False
                break
        
        if all_prepared:
            transaction.update_state(TransactionState.PREPARED)
        else:
            transaction.update_state(TransactionState.ABORTING)
        
        self._save_transaction(transaction)
        return all_prepared
    
    def commit(self, transaction_id: str, commit_functions: Dict[str, Callable]) -> bool:
        """
        Execute the commit phase of the three-phase commit protocol.
        
        Args:
            transaction_id: Transaction ID
            commit_functions: Dict mapping participant IDs to their commit functions
            
        Returns:
            True if all participants successfully committed, False otherwise
        """
        if transaction_id not in self.transactions:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        transaction = self.transactions[transaction_id]
        
        if transaction.state != TransactionState.PREPARED:
            logger.error(f"Cannot commit transaction {transaction_id} in state {transaction.state.value}")
            return False
        
        transaction.update_state(TransactionState.COMMITTING)
        self._save_transaction(transaction)
        
        # Execute commit functions for all participants
        all_committed = True
        for participant_id in transaction.participants:
            if participant_id in commit_functions:
                try:
                    result = commit_functions[participant_id](transaction)
                    if not result:
                        logger.error(f"Participant {participant_id} failed to commit transaction {transaction_id}")
                        all_committed = False
                except Exception as e:
                    logger.exception(f"Error committing transaction {transaction_id} for participant {participant_id}: {e}")
                    all_committed = False
        
        if all_committed:
            transaction.update_state(TransactionState.COMMITTED)
            # No need to save the transaction as it's complete
            self._cleanup_transaction(transaction_id)
        else:
            logger.critical(
                f"Transaction {transaction_id} commit failed after preparation phase. "
                "System may be in an inconsistent state."
            )
        
        return all_committed
    
    def abort(self, transaction_id: str, abort_functions: Dict[str, Callable]) -> bool:
        """
        Execute the abort phase of the three-phase commit protocol.
        
        Args:
            transaction_id: Transaction ID
            abort_functions: Dict mapping participant IDs to their abort functions
            
        Returns:
            True if all participants successfully aborted, False otherwise
        """
        if transaction_id not in self.transactions:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        transaction = self.transactions[transaction_id]
        transaction.update_state(TransactionState.ABORTING)
        self._save_transaction(transaction)
        
        # Execute abort functions for all participants
        all_aborted = True
        for participant_id in transaction.participants:
            if participant_id in abort_functions:
                try:
                    abort_functions[participant_id](transaction)
                except Exception as e:
                    logger.exception(f"Error aborting transaction {transaction_id} for participant {participant_id}: {e}")
                    all_aborted = False
        
        transaction.update_state(TransactionState.ABORTED)
        # Clean up transaction file
        self._cleanup_transaction(transaction_id)
        
        return all_aborted
    
    def get_transaction(self, transaction_id: str) -> Optional[Transaction]:
        """
        Get a transaction by ID.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Transaction if found, None otherwise
        """
        return self.transactions.get(transaction_id)
    
    def get_pending_transactions(self) -> List[Transaction]:
        """
        Get all pending transactions.
        
        Returns:
            List of pending transactions
        """
        return [
            transaction for transaction in self.transactions.values()
            if transaction.state != TransactionState.COMMITTED
            and transaction.state != TransactionState.ABORTED
        ]
    
    def _save_transaction(self, transaction: Transaction) -> None:
        """
        Save transaction to disk.
        
        Args:
            transaction: Transaction to save
        """
        file_path = os.path.join(self.log_dir, f"{transaction.transaction_id}.json")
        with open(file_path, "w") as f:
            f.write(transaction.to_json())
    
    def _cleanup_transaction(self, transaction_id: str) -> None:
        """
        Clean up transaction file.
        
        Args:
            transaction_id: Transaction ID to clean up
        """
        file_path = os.path.join(self.log_dir, f"{transaction_id}.json")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            
            # Also remove from memory
            if transaction_id in self.transactions:
                del self.transactions[transaction_id]
        except Exception as e:
            logger.exception(f"Error cleaning up transaction {transaction_id}: {e}")
    
    def _recover_transactions(self) -> None:
        """
        Recover transactions from disk.
        """
        try:
            if not os.path.exists(self.log_dir):
                return
            
            for filename in os.listdir(self.log_dir):
                if not filename.endswith(".json"):
                    continue
                
                file_path = os.path.join(self.log_dir, filename)
                try:
                    with open(file_path, "r") as f:
                        transaction = Transaction.from_json(f.read())
                        self.transactions[transaction.transaction_id] = transaction
                        logger.info(f"Recovered transaction {transaction.transaction_id} in state {transaction.state.value}")
                except Exception as e:
                    logger.exception(f"Error recovering transaction from {file_path}: {e}")
        except Exception as e:
            logger.exception(f"Error recovering transactions: {e}")


class TransactionParticipant:
    """
    Base class for transaction participants.
    """
    
    def __init__(self, participant_id: str):
        """
        Initialize transaction participant.
        
        Args:
            participant_id: Unique identifier for the participant
        """
        self.participant_id = participant_id
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the transaction.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning(f"Default prepare method called for {self.participant_id}")
        return True
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the transaction.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning(f"Default commit method called for {self.participant_id}")
        return True
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning(f"Default abort method called for {self.participant_id}")
        return True 