package spy

import (
	"github.com/ethereum/go-ethereum/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	golanglog "log"
	"os"
	"time"
)

/**
RLPX protocol (https://github.com/ethereum/devp2p/blob/master/rlpx.md)
0x01 Hello		[protocolVersion: P, clientId: B, capabilities, listenPort: P, nodeKey: B_64, ...]
0x02 Disconnect	[reason: P]
	Reason	Meaning
	0x00	Disconnect requested
	0x01	TCP sub-system error
	0x02	Breach of protocol, e.g. a malformed message, bad RLP, ...
	0x03	Useless peer
	0x04	Too many peers
	0x05	Already connected
	0x06	Incompatible P2P protocol version
	0x07	Null node identity received - this is automatically invalid
	0x08	Client quitting
	0x09	Unexpected identity in handshake
	0x0a	Identity is the same as this node (i.e. connected to itself)
	0x0b	Ping timeout
	0x10	Some other reason specific to a subprotocol
0x03 Ping 		[]
0x04 Pong		[]
*/

type Rlpx0x01Msg struct {
	ID            uint `gorm:"primarykey"`
	PeerID        string
	CreatedAt     uint
	Enr           string
	Enode         string
	NodeId        string
	Name          string
	Caps          string
	LocalAddress  string
	RemoteAddress string
	Inbound       bool
	Trusted       bool
	Static        bool
}

type Rlpx0x02Msg struct {
	ID         uint `gorm:"primarykey"`
	PeerID     string
	ReceivedAt time.Time
	Reason     string
}

type Rlpx0x03Msg struct {
	ID         uint `gorm:"primarykey"`
	PeerID     string
	ReceivedAt time.Time
}

type Rlpx0x04Msg struct {
	ID         uint `gorm:"primarykey"`
	PeerID     string
	ReceivedAt time.Time
}

type RlpxSpy struct {
	Channel0x01 chan *Rlpx0x01Msg
	Channel0x02 chan *Rlpx0x02Msg
	Channel0x03 chan *Rlpx0x03Msg
	Channel0x04 chan *Rlpx0x04Msg
}

func NewRlpxSpy(channelSize int) *RlpxSpy {
	spy := RlpxSpy{
		Channel0x01: make(chan *Rlpx0x01Msg, channelSize),
		Channel0x02: make(chan *Rlpx0x02Msg, channelSize),
		Channel0x03: make(chan *Rlpx0x03Msg, channelSize),
		Channel0x04: make(chan *Rlpx0x04Msg, channelSize),
	}

	go spy.execute()

	return &spy
}

func (r *RlpxSpy) Close() {
	close(r.Channel0x01)
	close(r.Channel0x02)
	close(r.Channel0x03)
	close(r.Channel0x04)
}

func (r *RlpxSpy) execute() {
	log.Info("Starting WireSpy Node")

	dsn := "host=localhost user=postgres password=password dbname=postgres port=5432 sslmode=disable"
	db, err := gorm.Open(
		postgres.Open(dsn),
		&gorm.Config{
			NamingStrategy: schema.NamingStrategy{TablePrefix: "ethereum_"},
			Logger: logger.New(golanglog.New(os.Stdout, "\r\n", golanglog.LstdFlags), logger.Config{
				SlowThreshold: 400 * time.Millisecond,
				LogLevel:      logger.Warn,
				Colorful:      true,
			}),
		})

	if err != nil {
		panic("failed to connect database")
	}

	// Table auto migration
	if db.AutoMigrate(&Rlpx0x01Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Rlpx0x02Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Rlpx0x03Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Rlpx0x04Msg{}) != nil {
		panic("Failed to migrate db")
	}

	log.Info("SPY - Rlpx postgres connected")

	//TODO: We can use a cache to optimise the code below, but lets keep it simple for now:
	for {
		select {
		case obj := <-r.Channel0x01:
			db.Create(&obj)
		case obj := <-r.Channel0x02:
			db.Create(&obj)
		case obj := <-r.Channel0x03:
			db.Create(&obj)
		case obj := <-r.Channel0x04:
			db.Create(&obj)
		default:
			continue
		}
	}
}
