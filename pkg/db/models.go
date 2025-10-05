package db

import (
	"time"

	"github.com/uptrace/bun"
)

type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID  int64  `bun:",pk,autoincrement"`
	DID string `bun:",notnull,unique"`

	Posts   []*Post   `bun:"rel:has-many,join:id=author_id"`
	Reposts []*Repost `bun:"rel:has-many,join:id=reposter_id"`
	Likes   []*Like   `bun:"rel:has-many,join:id=liker_id"`
}

type List struct {
	bun.BaseModel `bun:"table:lists,alias:l"`

	ID          int64  `bun:",pk,autoincrement"`
	URI         string `bun:",notnull,unique"`
	ListMembers []User `bun:"m2m:list_members,join:List=User"`
}

// https://bun.uptrace.dev/guide/relations.html#many-to-many-relation
type ListToUser struct {
	bun.BaseModel `bun:"table:list_members,alias:lm"`

	ListID int64 `bun:",pk"`
	List   *List `bun:"rel:belongs-to,join:list_id=id"`
	UserID int64 `bun:",pk"`
	User   *User `bun:"rel:belongs-to,join:user_id=id"`
}

type Post struct {
	bun.BaseModel `bun:"table:posts,alias:p"`

	ID  int64  `bun:",pk,autoincrement"`
	URI string `bun:",notnull,unique"`
	CID string `bun:",notnull"`

	AuthorID int64 `bun:",notnull"`
	Author   *User `bun:"rel:belongs-to,join:author_id=id"`

	CreatedAt time.Time `bun:",notnull"`
	IndexedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`

	ReplyParentID int64
	ReplyParent   *Post `bun:"rel:belongs-to,join:reply_parent_id=id"`

	ReplyRootID int64
	ReplyRoot   *Post `bun:"rel:belongs-to,join:reply_root_id=id"`

	Reposts []*Repost `bun:"rel:has-many,join:id=post_id"`
	Likes   []*Like   `bun:"rel:has-many,join:id=post_id"`
}

type Repost struct {
	bun.BaseModel `bun:"table:reposts,alias:rp"`

	ReposterID int64 `bun:",pk"`
	Reposter   *User `bun:"rel:belongs-to,join:reposter_id=id"`
	PostID     int64 `bun:",pk"`
	Post       *Post `bun:"rel:belongs-to,join:post_id=id"`

	CreatedAt time.Time `bun:",notnull"`
	IndexedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	URI       string    `bun:",notnull,unique"`
}

type Like struct {
	bun.BaseModel `bun:"table:likes,alias:lp"`

	LikerID int64 `bun:",pk"`
	Liker   *User `bun:"rel:belongs-to,join:liker_id=id"`
	PostID  int64 `bun:",pk"`
	Post    *Post `bun:"rel:belongs-to,join:post_id=id"`

	CreatedAt time.Time `bun:",notnull"`
	IndexedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	URI       string    `bun:",notnull,unique"`
}

type SubscriptionState struct {
	bun.BaseModel `bun:"table:subscription_state,alias:ss"`

	Service string    `bun:",notnull,unique"`
	Cursor  int64     `bun:",notnull,default:1"`
	Lag     time.Time `bun:",notnull,default:current_timestamp"`
}
