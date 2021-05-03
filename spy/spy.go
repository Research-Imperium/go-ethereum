package spy

import (
	"encoding/hex"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"gorm.io/gorm/clause"
	"time"
)
import "gorm.io/gorm"
import "gorm.io/driver/postgres"

type EthereumBlock struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Hash         string `gorm:"index"`
	Code         uint
	ReceivedTime time.Time
	BlockNumber  uint
}

type EthereumTransaction struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Hash         string `gorm:"index"`
	Code         uint
	ReceivedTime time.Time
}

type EthereumTransactionContent struct {
	Hash     string `gorm:"primaryKey"`
	To       string
	From     string
	Nonce    uint
	Value    string
	GasPrice string
	Gas      uint
	Data     string
}

type EthereumPeer struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Version      int
	IP           string
	ReceivedTime time.Time
}

type Spy struct {
	peerCh      chan *EthereumPeer
	blockCh     chan *EthereumBlock
	txCh        chan *EthereumTransaction
	txContentCh chan *EthereumTransactionContent
}

func NewSpy() *Spy {
	spy := Spy{
		peerCh:      make(chan *EthereumPeer, 10000),
		blockCh:     make(chan *EthereumBlock, 10000),
		txCh:        make(chan *EthereumTransaction, 10000),
		txContentCh: make(chan *EthereumTransactionContent, 10000),
	}
	go spy.execute()
	return &spy
}

// execute is called on initialization
func (w *Spy) execute() {
	// initiate all databases
	dsn := "host=localhost user=postgres password=password dbname=postgres port=5433 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	if db.AutoMigrate(&EthereumBlock{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumTransaction{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumPeer{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumTransactionContent{}) != nil {
		panic("Failed to migrate db")
	}

	// transaction cache
	var transactionCountCacheHashes []string
	transactionCountCache := map[string]int64{}

	max := int64(20)
	maxTransactionCacheCount := 10000
	batchSize := 1000

	var blockBatch []*EthereumBlock
	var transactionBatch []*EthereumTransaction

	for {
		select {
		case block := <-w.blockCh:
			blockBatch = append(blockBatch, block)
			// batch insert if it is over the batch size
			if len(blockBatch) > batchSize {
				db.Create(&blockBatch)
				blockBatch = []*EthereumBlock{}
			}
		case tx := <-w.txCh:
			_, exists := transactionCountCache[tx.Hash]
			if !exists {
				var result []EthereumTransaction
				var count int64
				db.Where("hash = ?", tx.Hash).Find(&result).Count(&count)
				transactionCountCacheHashes = append(transactionCountCacheHashes, tx.Hash)
				transactionCountCache[tx.Hash] = count
				if len(transactionCountCacheHashes) > maxTransactionCacheCount {
					toDelete := transactionCountCacheHashes[0]
					transactionCountCacheHashes = transactionCountCacheHashes[1:]
					delete(transactionCountCache, toDelete)
				}
			}
			if transactionCountCache[tx.Hash] < max {
				transactionBatch = append(transactionBatch, tx)
				transactionCountCache[tx.Hash] += 1
				// batch insert if it is over the batch size
				if len(transactionBatch) > batchSize {
					db.Create(&transactionBatch)
					transactionBatch = []*EthereumTransaction{}
				}
			}
		case peer := <-w.peerCh:
			db.Create(&peer)
		case content := <-w.txContentCh:
			db.Clauses(clause.OnConflict{DoNothing: true}).Create(&content)
		default:
			continue
		}
	}
}

// closing all channels
func (w *Spy) Close() {
	close(w.peerCh)
	close(w.blockCh)
	close(w.txCh)
	close(w.txContentCh)
}

// eth protocol message codes
const (
	StatusMsg          = 0x00
	NewBlockHashesMsg  = 0x01
	TransactionMsg     = 0x02
	GetBlockHeadersMsg = 0x03
	BlockHeadersMsg    = 0x04
	GetBlockBodiesMsg  = 0x05
	BlockBodiesMsg     = 0x06
	NewBlockMsg        = 0x07
	GetNodeDataMsg     = 0x0d
	NodeDataMsg        = 0x0e
	GetReceiptsMsg     = 0x0f
	ReceiptsMsg        = 0x10

	// New protocol message codes introduced in eth65
	//
	// Previously these message ids were used by some legacy and unsupported
	// eth protocols, reown them here.
	NewPooledTransactionHashesMsg = 0x08
	GetPooledTransactionsMsg      = 0x09
	PooledTransactionsMsg         = 0x0a
)

func GetMsgCodeText(msg p2p.Msg) string {
	switch msg.Code {
	case StatusMsg:
		return "StatusMsg"
	case NewBlockHashesMsg:
		return "NewBlockHashesMsg"
	case TransactionMsg:
		return "TransactionMsg"
	case GetBlockHeadersMsg:
		return "GetBlockHeadersMsg"
	case BlockHeadersMsg:
		return "BlockHeadersMsg"
	case GetBlockBodiesMsg:
		return "GetBlockBodiesMsg"
	case BlockBodiesMsg:
		return "BlockBodiesMsg"
	case NewBlockMsg:
		return "NewBlockMsg"
	case GetNodeDataMsg:
		return "GetNodeDataMsg"
	case NodeDataMsg:
		return "NodeDataMsg"
	case GetReceiptsMsg:
		return "GetReceiptsMsg"
	case ReceiptsMsg:
		return "ReceiptsMsg"
	case NewPooledTransactionHashesMsg:
		return "NewPooledTransactionHashesMsg"
	case GetPooledTransactionsMsg:
		return "GetPooledTransactionsMsg"
	case PooledTransactionsMsg:
		return "PooledTransactionsMsg"
	default:
		return ""
	}
}

func (w *Spy) HandleBlockMsg(peerID string, msg p2p.Msg, hash string, blockNumber uint64) {
	w.blockCh <- &EthereumBlock{
		PeerID:       peerID,
		Hash:         hash,
		Code:         uint(msg.Code),
		ReceivedTime: msg.ReceivedAt,
		BlockNumber:  uint(blockNumber),
	}
}

func (w *Spy) HandleTxMsg(peerID string, msg p2p.Msg, hash string) {
	w.txCh <- &EthereumTransaction{
		PeerID:       peerID,
		Hash:         hash,
		Code:         uint(msg.Code),
		ReceivedTime: msg.ReceivedAt,
	}
}

func (w *Spy) HandlePeerMsg(peerID string, version int, ip string) {
	w.peerCh <- &EthereumPeer{
		PeerID:       peerID,
		Version:      version,
		IP:           ip,
		ReceivedTime: time.Now(),
	}
}

func (w *Spy) HandleTxContent(hash string, msg *types.Message) {
	var toAddress string
	if msg.To() == nil {
		toAddress = ""
	} else {
		toAddress = msg.To().Hex()
	}

	w.txContentCh <- &EthereumTransactionContent{
		Hash:     hash,
		To:       toAddress,
		From:     msg.From().Hex(),
		Nonce:    uint(msg.Nonce()),
		Value:    msg.Value().String(),
		GasPrice: msg.GasPrice().String(),
		Gas:      uint(msg.Gas()),
		Data:     hex.EncodeToString(msg.Data()),
	}
}
