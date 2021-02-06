package spy

import (
	"encoding/hex"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"time"
)
import "gorm.io/gorm"
import "gorm.io/driver/postgres"

type SpyBlock struct {
	gorm.Model
	PeerID       string
	Hash         string
	Code         uint64
	CodeText     string
	ReceivedTime time.Time
	BlockNumber  uint64
}

type SpyTransaction struct {
	gorm.Model
	PeerID       string
	Hash         string
	Code         uint64
	CodeText     string
	ReceivedTime time.Time
}

type SpyPeer struct {
	gorm.Model
	PeerID  string
	Version int
	IP      string
}

type SpyTransactionContent struct {
	gorm.Model
	Hash     string
	To       string
	From     string
	Nonce    uint64
	Value    string
	GasPrice string
	Gas      uint64
	Data     string
}

type Spy struct {
	peerCh      chan *SpyPeer
	blockCh     chan *SpyBlock
	txCh        chan *SpyTransaction
	txContentCh chan *SpyTransactionContent
}

func NewSpy() *Spy {
	spy := Spy{
		peerCh:      make(chan *SpyPeer, 1000),
		blockCh:     make(chan *SpyBlock, 1000),
		txCh:        make(chan *SpyTransaction, 1000),
		txContentCh: make(chan *SpyTransactionContent, 1000),
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

	if db.AutoMigrate(&SpyBlock{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&SpyTransaction{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&SpyPeer{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&SpyTransactionContent{}) != nil {
		panic("Failed to migrate db")
	}

	for {
		select {
		case block := <-w.blockCh:
			db.Create(&block)
		case tx := <-w.txCh:
			db.Create(&tx)
		case peer := <-w.peerCh:
			db.Create(&peer)
		case content := <-w.txContentCh:
			db.Create(&content)
			//db.Clauses(
			//	clause.OnConflict{
			//		Columns:   []clause.Column{{Name: "Hash"}},
			//		DoNothing: true},
			//).Create(&content)
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
	w.blockCh <- &SpyBlock{
		PeerID:       peerID,
		Hash:         hash,
		Code:         msg.Code,
		CodeText:     GetMsgCodeText(msg),
		ReceivedTime: msg.ReceivedAt,
		BlockNumber:  blockNumber,
	}
}

func (w *Spy) HandleTxMsg(peerID string, msg p2p.Msg, hash string) {
	w.txCh <- &SpyTransaction{
		PeerID:       peerID,
		Hash:         hash,
		Code:         msg.Code,
		CodeText:     GetMsgCodeText(msg),
		ReceivedTime: msg.ReceivedAt,
	}
}

func (w *Spy) HandlePeerMsg(peerID string, version int, ip string) {
	w.peerCh <- &SpyPeer{
		PeerID:  peerID,
		Version: version,
		IP:      ip,
	}
}

func (w *Spy) HandleTxContent(hash string, msg *types.Message) {
	w.txContentCh <- &SpyTransactionContent{
		Hash:     hash,
		To:       msg.To().Hex(),
		From:     msg.From().Hex(),
		Nonce:    msg.Nonce(),
		Value:    msg.Value().String(),
		GasPrice: msg.GasPrice().String(),
		Gas:      msg.Gas(),
		Data:     hex.EncodeToString(msg.Data()),
	}
}
