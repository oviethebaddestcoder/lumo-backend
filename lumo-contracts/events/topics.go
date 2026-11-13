package events

// Kafka topics. One topic per domain; the key column says what each is keyed by.
// Compacted topics hold the latest state per key and let consumers rebuild
// local projections from scratch.
const (
	TopicIdentityUsers = "identity.users" // key: user_id   (compacted) user state
	TopicSocialEvents  = "social.events"  // key: target id (video or user)
	TopicMediaVideos   = "media.videos"   // key: video_id  (compacted) video state
	TopicMediaEvents   = "media.events"   // key: video_id
	TopicMediaJobs     = "media.jobs"     // key: video_id  transcode jobs
	TopicMediaResults  = "media.job_results"
	TopicLiveEvents    = "live.events"   // key: room_id
	TopicLivePK        = "live.pk"       // key: pk_id
	TopicChatMessages  = "chat.messages" // key: room_id
	TopicWalletGifts   = "wallet.gifts"  // key: receiver_id (one host's gifts stay ordered)
	TopicWalletEvents  = "wallet.events" // key: user_id   purchases, payouts, credits
	TopicModeration    = "moderation.actions"
	TopicFeedSignals   = "feed.impressions" // key: user_id
	TopicDMMessages    = "dm.messages"      // key: conversation_id
)

// DLQ returns the dead-letter topic for a topic.
func DLQ(topic string) string { return topic + ".dlq" }

// AllTopics is used by infra tooling to create topics.
var AllTopics = []struct {
	Name      string
	Compacted bool
	Retention string // human readable; infra converts
}{
	{TopicIdentityUsers, true, "forever"},
	{TopicSocialEvents, false, "7d"},
	{TopicMediaVideos, true, "forever"},
	{TopicMediaEvents, false, "7d"},
	{TopicMediaJobs, false, "3d"},
	{TopicMediaResults, false, "3d"},
	{TopicLiveEvents, false, "7d"},
	{TopicLivePK, false, "7d"},
	{TopicChatMessages, false, "3d"},
	{TopicWalletGifts, false, "30d"},
	{TopicWalletEvents, false, "30d"},
	{TopicModeration, false, "30d"},
	{TopicFeedSignals, false, "3d"},
	{TopicDMMessages, false, "3d"},
}
