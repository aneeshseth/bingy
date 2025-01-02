-- name: GetUser :one
SELECT * FROM users
WHERE id = $1
LIMIT 1;

-- name: InsertUser :exec
INSERT INTO users (first_name, last_name, bio, latitude, longitude)
VALUES (?, ?, ?, ?, ?);

-- name: InsertMatch :exec
INSERT INTO matches (user_id_1, user_id_2)
VALUES (?, ?);

-- name: InsertSwipe :exec
INSERT INTO swipes (user_swiped, user_swiped_on, swipe_type)
VALUES (?, ?, ?);
