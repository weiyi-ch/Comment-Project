# comment-tutor

`comment-tutor` is the tutor-facing BFF for `comment-service`.

It accepts HTTP requests from the student client, extracts the student identity from headers, and calls `comment-service` through the generated gRPC clients in `api/comment/v1`.
It keeps its own protocol SDK under `api/comment/v1`, generated from the same `comment.proto` contract used by `comment-service`.

Runtime service lookup uses Consul discovery:

```text
comment-tutor -> Consul -> comment-service -> gRPC
```

## Run

Start Consul first. Then start `comment-service` so it registers itself as `comment-service`.
After that, run:

```bash
go run ./cmd/comment-tutor -conf ./configs/config.yaml
```

Default BFF address:

```text
http://127.0.0.1:8081
```

Default discovery config:

```yaml
registry:
  consul:
    address: 127.0.0.1:8500
client:
  comment_service:
    service_name: comment-service
```

## Auth headers

For local development, pass one of:

```text
x-user-id: 1001
x-role: student
```

or:

```text
Authorization: Bearer student:1001
```

In production, replace `internal/auth` with real JWT/session validation while keeping the same user context boundary.

## Main routes

```text
GET    /api/student/posts/{post_id}
GET    /api/student/posts/{post_id}/comments
POST   /api/student/posts/{post_id}/comments
GET    /api/student/comments
GET    /api/student/comments/{comment_id}
DELETE /api/student/comments/{comment_id}
POST   /api/student/posts/{post_id}/like
DELETE /api/student/posts/{post_id}/like
GET    /api/student/search/posts
GET    /api/student/search/comments
```
