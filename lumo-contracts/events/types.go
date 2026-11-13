package events

import "time"

// Event types: domain.entity.verb
const (
	UserRegistered = "identity.user.registered"
	UserUpdated    = "identity.user.updated"
	UserDeleted    = "identity.user.deleted"
	UserSuspended  = "identity.user.suspended"

	Followed       = "social.follow.created"
	Unfollowed     = "social.follow.deleted"
	Liked          = "social.like.created"
	Unliked        = "social.like.deleted"
	Commented      = "social.comment.created"
	CommentDeleted = "social.comment.deleted"
	Blocked        = "social.block.created"
	Unblocked      = "social.block.deleted"
	Reported       = "social.report.created"

	VideoState    = "media.video.state" // full snapshot on media.videos
	VideoUploaded = "media.video.uploaded"
	VideoReady    = "media.video.ready"
	VideoFailed   = "media.video.failed"
	VideoRemoved  = "media.video.removed"
	TranscodeJob  = "media.job.requested"
	TranscodeDone = "media.job.completed"

	RoomStarted = "live.room.started"
	RoomEnded   = "live.room.ended"
	ReplayReady = "live.replay.ready"
	PKStarted   = "live.pk.started"
	PKEnded     = "live.pk.ended"

	ChatSent = "chat.message.sent"

	GiftSent         = "wallet.gift.sent"
	CoinsPurchased   = "wallet.coins.purchased"
	CoinsReversed    = "wallet.coins.reversed"
	PayoutRequested  = "wallet.payout.requested"
	PayoutSettled    = "wallet.payout.settled"
	DiamondsCredited = "wallet.diamonds.credited"

	ModerationAction = "moderation.action.taken"

	Impression = "feed.impression.recorded"

	DMSent = "dm.message.sent"
)

// ---- identity ----

// UserPublic is the public projection of a user that other services may cache.
type UserPublic struct {
	ID          string    `json:"id"`
	Handle      string    `json:"handle"`
	DisplayName string    `json:"display_name"`
	AvatarURL   string    `json:"avatar_url,omitempty"`
	Bio         string    `json:"bio,omitempty"`
	Verified    bool      `json:"verified"`
	Adult       bool      `json:"adult"`
	Status      string    `json:"status"` // active | suspended | deleted
	CreatedAt   time.Time `json:"created_at"`
}

type UserRegisteredData struct {
	User   UserPublic `json:"user"`
	Method string     `json:"method"` // phone | email | google | apple | facebook | x | line | kakao
}

type UserUpdatedData struct {
	User UserPublic `json:"user"`
}

type UserDeletedData struct {
	UserID string `json:"user_id"`
}

// ---- social ----

type FollowData struct {
	FollowerID string `json:"follower_id"`
	FolloweeID string `json:"followee_id"`
}

type LikeData struct {
	UserID  string `json:"user_id"`
	VideoID string `json:"video_id"`
	OwnerID string `json:"owner_id"`
}

type CommentData struct {
	CommentID string `json:"comment_id"`
	VideoID   string `json:"video_id"`
	OwnerID   string `json:"owner_id"`
	UserID    string `json:"user_id"`
	ParentID  string `json:"parent_id,omitempty"`
	Text      string `json:"text,omitempty"`
}

type BlockData struct {
	BlockerID string `json:"blocker_id"`
	BlockedID string `json:"blocked_id"`
}

type ReportData struct {
	ReportID   string `json:"report_id"`
	ReporterID string `json:"reporter_id"`
	TargetType string `json:"target_type"` // user | video | comment | room | message
	TargetID   string `json:"target_id"`
	Reason     string `json:"reason"`
	Details    string `json:"details,omitempty"`
}

// ---- media ----

type VideoStateData struct {
	ID           string     `json:"id"`
	OwnerID      string     `json:"owner_id"`
	Kind         string     `json:"kind"`   // short | long | replay
	Status       string     `json:"status"` // uploading | processing | ready | failed | removed
	Visibility   string     `json:"visibility"`
	Title        string     `json:"title,omitempty"`
	Description  string     `json:"description,omitempty"`
	Hashtags     []string   `json:"hashtags,omitempty"`
	DurationMS   int64      `json:"duration_ms,omitempty"`
	Width        int        `json:"width,omitempty"`
	Height       int        `json:"height,omitempty"`
	HLSURL       string     `json:"hls_url,omitempty"`
	ThumbnailURL string     `json:"thumbnail_url,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
}

type TranscodeJobData struct {
	JobID        string `json:"job_id"`
	VideoID      string `json:"video_id"`
	Kind         string `json:"kind"`
	SourceKey    string `json:"source_key"`
	OutputPrefix string `json:"output_prefix"`
}

type TranscodeResultData struct {
	JobID      string `json:"job_id"`
	VideoID    string `json:"video_id"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	MasterKey  string `json:"master_key,omitempty"`
	ThumbKey   string `json:"thumb_key,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
}

// ---- live ----

type RoomStartedData struct {
	RoomID    string    `json:"room_id"`
	HostID    string    `json:"host_id"`
	Title     string    `json:"title"`
	Category  string    `json:"category"`
	StartedAt time.Time `json:"started_at"`
}

type RoomEndedData struct {
	RoomID      string    `json:"room_id"`
	HostID      string    `json:"host_id"`
	EndedAt     time.Time `json:"ended_at"`
	PeakViewers int64     `json:"peak_viewers"`
	GiftCoins   int64     `json:"gift_coins"`
	Reason      string    `json:"reason"`
}

type ReplayReadyData struct {
	RoomID    string `json:"room_id"`
	HostID    string `json:"host_id"`
	Title     string `json:"title"`
	ObjectKey string `json:"object_key"`
}

type PKStartedData struct {
	PKID   string    `json:"pk_id"`
	RoomA  string    `json:"room_a"`
	RoomB  string    `json:"room_b"`
	HostA  string    `json:"host_a"`
	HostB  string    `json:"host_b"`
	EndsAt time.Time `json:"ends_at"`
}

type PKEndedData struct {
	PKID       string `json:"pk_id"`
	RoomA      string `json:"room_a"`
	RoomB      string `json:"room_b"`
	ScoreA     int64  `json:"score_a"`
	ScoreB     int64  `json:"score_b"`
	WinnerRoom string `json:"winner_room,omitempty"` // empty = draw
}

// ---- chat ----

type ChatMessageData struct {
	ID     string    `json:"id"`
	RoomID string    `json:"room_id"`
	UserID string    `json:"user_id"`
	Text   string    `json:"text"`
	SentAt time.Time `json:"sent_at"`
}

// ---- wallet ----

type GiftSentData struct {
	GiftSendID string    `json:"gift_send_id"`
	SenderID   string    `json:"sender_id"`
	ReceiverID string    `json:"receiver_id"`
	RoomID     string    `json:"room_id,omitempty"`
	VideoID    string    `json:"video_id,omitempty"`
	GiftID     string    `json:"gift_id"`
	Qty        int       `json:"qty"`
	Coins      int64     `json:"coins"`
	Diamonds   int64     `json:"diamonds"`
	SentAt     time.Time `json:"sent_at"`
	// FromBackpack gifts were free (daily rewards): they count toward PK
	// scores and goals but pay the receiver no diamonds.
	FromBackpack bool `json:"from_backpack,omitempty"`
}

type CoinsPurchasedData struct {
	PurchaseID string `json:"purchase_id"`
	UserID     string `json:"user_id"`
	Channel    string `json:"channel"` // google_play | app_store | paystack
	ProductID  string `json:"product_id"`
	StoreRef   string `json:"store_ref"` // purchase token / transaction id / reference
	Coins      int64  `json:"coins"`
}

type CoinsReversedData struct {
	PurchaseID string `json:"purchase_id"`
	UserID     string `json:"user_id"`
	Coins      int64  `json:"coins"`
	Reason     string `json:"reason"`
}

type PayoutRequestedData struct {
	PayoutID string `json:"payout_id"`
	UserID   string `json:"user_id"`
	Diamonds int64  `json:"diamonds"`
}

// PayoutSettledData is sent when a cash-out is paid, rejected or fails.
type PayoutSettledData struct {
	PayoutID   string `json:"payout_id"`
	UserID     string `json:"user_id"`
	Diamonds   int64  `json:"diamonds"`
	AmountKobo int64  `json:"amount_kobo"`
	Status     string `json:"status"` // paid | rejected | failed
	Reason     string `json:"reason,omitempty"`
}

type DiamondsCreditedData struct {
	ReceiverID string `json:"receiver_id"`
	Diamonds   int64  `json:"diamonds"`
	Count      int    `json:"count"`
}

// ---- moderation ----

type ModerationActionData struct {
	ActionID   string     `json:"action_id"`
	TargetType string     `json:"target_type"` // user | video | comment | room | message
	TargetID   string     `json:"target_id"`
	Action     string     `json:"action"` // ban | unban | remove | restore | end_room | mute
	Reason     string     `json:"reason"`
	ActorID    string     `json:"actor_id"`
	Until      *time.Time `json:"until,omitempty"`
	RoomID     string     `json:"room_id,omitempty"`
}

// ---- feed ----

type ImpressionData struct {
	UserID  string    `json:"user_id"`
	VideoID string    `json:"video_id"`
	WatchMS int64     `json:"watch_ms"`
	At      time.Time `json:"at"`
}

// ---- direct messages ----

// DMSentData lets the notifier push "new message" to the recipient.
type DMSentData struct {
	MessageID      string    `json:"message_id"`
	ConversationID string    `json:"conversation_id"`
	SenderID       string    `json:"sender_id"`
	RecipientID    string    `json:"recipient_id"`
	Preview        string    `json:"preview"`
	SentAt         time.Time `json:"sent_at"`
}
