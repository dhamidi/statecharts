package main

import (
	"fmt"
	"sort"
)

const (
	directionUp    = "up"
	directionDown  = "down"
	directionLeft  = "left"
	directionRight = "right"

	actionUp    = "up"
	actionDown  = "down"
	actionLeft  = "left"
	actionRight = "right"
	actionShoot = "shoot"
)

type tile struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type creature struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Color        string `json:"color"`
	X            int    `json:"x"`
	Y            int    `json:"y"`
	Facing       string `json:"facing"`
	Health       int    `json:"health"`
	Power        int    `json:"power"`
	Score        int    `json:"score"`
	LastSequence uint64 `json:"last_sequence"`
	Bot          bool   `json:"bot"`
	Connected    bool   `json:"connected"`
}

type projectile struct {
	Owner  string `json:"owner"`
	X      int    `json:"x"`
	Y      int    `json:"y"`
	DX     int    `json:"dx"`
	DY     int    `json:"dy"`
	Damage int    `json:"damage"`
	TTL    int    `json:"ttl"`
}

type powerup struct {
	X    int    `json:"x"`
	Y    int    `json:"y"`
	Kind string `json:"kind"`
}

type playerInput struct {
	Player   string `json:"player"`
	Lease    string `json:"lease,omitempty"`
	Sequence uint64 `json:"sequence"`
	Action   string `json:"action"`
}

type world struct {
	Width       int                 `json:"width"`
	Height      int                 `json:"height"`
	Tick        uint64              `json:"tick"`
	RandomState uint64              `json:"random_state"`
	Creatures   map[string]creature `json:"creatures"`
	Projectiles []projectile        `json:"projectiles"`
	Powerups    []powerup           `json:"powerups"`
	Walls       []tile              `json:"walls"`
}

type arenaSnapshot struct {
	Match       string       `json:"match"`
	Revision    string       `json:"revision"`
	Tick        uint64       `json:"tick"`
	Width       int          `json:"width"`
	Height      int          `json:"height"`
	Creatures   []creature   `json:"creatures"`
	Projectiles []projectile `json:"projectiles"`
	Powerups    []powerup    `json:"powerups"`
	Walls       []tile       `json:"walls"`
}

func newWorld(width, height int, seed uint64) world {
	if seed == 0 {
		seed = 1
	}
	w := world{Width: width, Height: height, RandomState: seed, Creatures: map[string]creature{}}
	for x := range width {
		w.Walls = append(w.Walls, tile{X: x, Y: 0}, tile{X: x, Y: height - 1})
	}
	for y := 1; y < height-1; y++ {
		w.Walls = append(w.Walls, tile{X: 0, Y: y}, tile{X: width - 1, Y: y})
	}
	for y := 4; y < height-2; y += 4 {
		for x := 4; x < width-2; x += 4 {
			w.Walls = append(w.Walls, tile{X: x, Y: y})
		}
	}
	sort.Slice(w.Walls, func(i, j int) bool {
		if w.Walls[i].Y != w.Walls[j].Y {
			return w.Walls[i].Y < w.Walls[j].Y
		}
		return w.Walls[i].X < w.Walls[j].X
	})
	return w
}

func (w *world) addPlayer(id, name, color string, bot bool) bool {
	if current, ok := w.Creatures[id]; ok {
		current.Name, current.Color, current.Bot, current.Connected = name, color, bot, true
		w.Creatures[id] = current
		return false
	}
	x, y := w.openCell(uint64(len(w.Creatures)))
	w.Creatures[id] = creature{ID: id, Name: name, Color: color, X: x, Y: y, Facing: directionRight, Health: 3, Bot: bot, Connected: true}
	return true
}

func (w *world) openCell(offset uint64) (int, int) {
	area := max(1, (w.Width-2)*(w.Height-2))
	for step := range area {
		index := int((offset*7 + uint64(step)) % uint64(area))
		x := 1 + index%(w.Width-2)
		y := 1 + index/(w.Width-2)
		if !w.blocked(x, y, "") && !w.hasPowerup(x, y) {
			return x, y
		}
	}
	return 1, 1
}

func (w *world) applyInput(input playerInput, movement int) bool {
	actor, ok := w.Creatures[input.Player]
	if !ok || input.Sequence <= actor.LastSequence {
		return false
	}
	actor.LastSequence = input.Sequence
	if movement < 1 {
		movement = 1
	}
	switch input.Action {
	case actionUp, actionDown, actionLeft, actionRight:
		actor.Facing = input.Action
		dx, dy := directionVector(input.Action)
		for range movement {
			nextX, nextY := actor.X+dx, actor.Y+dy
			if w.blocked(nextX, nextY, actor.ID) {
				break
			}
			actor.X, actor.Y = nextX, nextY
			w.collect(&actor)
		}
	case actionShoot:
		dx, dy := directionVector(actor.Facing)
		w.Projectiles = append(w.Projectiles, projectile{Owner: actor.ID, X: actor.X, Y: actor.Y, DX: dx, DY: dy, Damage: 1 + actor.Power, TTL: max(w.Width, w.Height)})
	default:
		return false
	}
	w.Creatures[input.Player] = actor
	return true
}

func directionVector(direction string) (int, int) {
	switch direction {
	case directionUp:
		return 0, -1
	case directionDown:
		return 0, 1
	case directionLeft:
		return -1, 0
	default:
		return 1, 0
	}
}

func (w *world) advance() {
	w.Tick++
	next := make([]projectile, 0, len(w.Projectiles))
	for _, shot := range w.Projectiles {
		shot.X += shot.DX
		shot.Y += shot.DY
		shot.TTL--
		if shot.TTL <= 0 || w.wallAt(shot.X, shot.Y) {
			continue
		}
		victimID := w.creatureAt(shot.X, shot.Y, shot.Owner)
		if victimID == "" {
			next = append(next, shot)
			continue
		}
		victim := w.Creatures[victimID]
		victim.Health -= shot.Damage
		if victim.Health <= 0 {
			owner := w.Creatures[shot.Owner]
			owner.Score += 10
			w.Creatures[shot.Owner] = owner
			victim.Health, victim.Power = 3, 0
			victim.X, victim.Y = w.openCell(w.nextRandom())
		}
		w.Creatures[victimID] = victim
	}
	w.Projectiles = next
	if w.Tick%30 == 0 && len(w.Powerups) < 4 {
		x, y := w.openCell(w.nextRandom())
		w.Powerups = append(w.Powerups, powerup{X: x, Y: y, Kind: "charge"})
	}
}

func (w *world) collect(actor *creature) {
	remaining := w.Powerups[:0]
	for _, item := range w.Powerups {
		if item.X == actor.X && item.Y == actor.Y {
			actor.Power++
			actor.Score += 5
			continue
		}
		remaining = append(remaining, item)
	}
	w.Powerups = remaining
}

func (w *world) blocked(x, y int, except string) bool {
	return w.wallAt(x, y) || w.creatureAt(x, y, except) != ""
}

func (w *world) wallAt(x, y int) bool {
	for _, wall := range w.Walls {
		if wall.X == x && wall.Y == y {
			return true
		}
	}
	return false
}

func (w *world) creatureAt(x, y int, except string) string {
	ids := make([]string, 0, len(w.Creatures))
	for id := range w.Creatures {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		actor := w.Creatures[id]
		if id != except && actor.X == x && actor.Y == y {
			return id
		}
	}
	return ""
}

func (w *world) hasPowerup(x, y int) bool {
	for _, item := range w.Powerups {
		if item.X == x && item.Y == y {
			return true
		}
	}
	return false
}

func (w *world) nextRandom() uint64 {
	x := w.RandomState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	w.RandomState = x
	return x
}

func (w *world) snapshot(match, revision string) arenaSnapshot {
	creatures := make([]creature, 0, len(w.Creatures))
	for _, actor := range w.Creatures {
		creatures = append(creatures, actor)
	}
	sort.Slice(creatures, func(i, j int) bool { return creatures[i].ID < creatures[j].ID })
	return arenaSnapshot{
		Match: match, Revision: revision, Tick: w.Tick, Width: w.Width, Height: w.Height,
		Creatures: creatures, Projectiles: append([]projectile{}, w.Projectiles...),
		Powerups: append([]powerup{}, w.Powerups...), Walls: append([]tile{}, w.Walls...),
	}
}

func snapshotPlayer(snapshot arenaSnapshot, id string) creature {
	for _, actor := range snapshot.Creatures {
		if actor.ID == id {
			return actor
		}
	}
	return creature{}
}

func (w world) validate() error {
	if w.Width < 5 || w.Height < 5 {
		return fmt.Errorf("arena dimensions must be at least 5x5")
	}
	return nil
}
