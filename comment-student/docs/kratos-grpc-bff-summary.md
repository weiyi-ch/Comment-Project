# Kratos gRPC and BFF Knowledge Summary

## 1. Project Problem

In this project, `comment-service` provides comment, post, like, and search capabilities. `comment-student` is the student-facing BFF. The important question is:

```text
How does a frontend HTTP request enter comment-student,
become a gRPC request,
reach comment-service,
and finally call the service method we wrote?
```

The complete chain involves:

```text
proto contract
 -> generated gRPC/HTTP code
 -> service implementation
 -> RegisterXXXServer
 -> Wire dependency injection
 -> Kratos App
 -> Consul registration/discovery
 -> BFF generated client stub
 -> gRPC remote call
```

## 2. Kratos Server Side: Proto and Service Implementation

Kratos is a proto-first microservice framework. The `comment.proto` file defines the service contract:

```proto
service StudentService {
  rpc GetPostDetailStudent(GetPostDetailRequest) returns (GetPostDetailReply);
  rpc CreateComment(CreateCommentRequest) returns (CreateCommentReply);
}
```

The proto file defines:

```text
service name
method name
request type
reply type
HTTP mapping
validation rules
```

It is only the contract. It is not business logic.

After code generation, several files are produced:

```text
comment.pb.go
 -> request/reply Go structs

comment_grpc.pb.go
 -> gRPC client stub
 -> server interface
 -> generated handler
 -> RegisterXXXServer function

comment_http.pb.go
 -> HTTP route mapping based on google.api.http annotations

comment.pb.validate.go
 -> validation logic based on validate rules
```

The most important generated file for gRPC is `comment_grpc.pb.go`.

It contains a generated server interface similar to:

```go
type StudentServiceServer interface {
    GetPostDetailStudent(context.Context, *GetPostDetailRequest) (*GetPostDetailReply, error)
    CreateComment(context.Context, *CreateCommentRequest) (*CreateCommentReply, error)
}
```

This means:

```text
If a Go object wants to become the server implementation of StudentService,
it must implement these methods.
```

The service implementation we write is in `internal/service/student.go`:

```go
type StudentService struct {
    pb.UnimplementedStudentServiceServer
    uc *biz.StudentUsecase
}

func (s *StudentService) GetPostDetailStudent(
    ctx context.Context,
    req *pb.GetPostDetailRequest,
) (*pb.GetPostDetailReply, error) {
    // service -> biz -> data
}
```

Go uses implicit interfaces. If the method signatures match, then:

```text
*StudentService implements generated StudentServiceServer
```

The key relationship is:

```text
generated code defines the interface
our service code implements the interface
RegisterXXXServer registers our implementation into the gRPC Server
```

## 3. RegisterXXXServer and FullMethodName Dispatch

When `comment-service` starts, it registers the service implementation:

```go
v1.RegisterStudentServiceServer(srv, studentService)
```

This registration puts two things into the gRPC Server:

```text
1. generated ServiceDesc
2. our studentService implementation object
```

The generated `ServiceDesc` describes:

```text
service name
method names
method handlers
```

Conceptually:

```text
StudentService_ServiceDesc:
  ServiceName: "api.comment.v1.StudentService"
  Methods:
    GetPostDetailStudent -> _StudentService_GetPostDetailStudent_Handler
    CreateComment        -> _StudentService_CreateComment_Handler
```

After registration, the server has a mapping:

```text
/api.comment.v1.StudentService/GetPostDetailStudent
 -> _StudentService_GetPostDetailStudent_Handler
 -> studentService.GetPostDetailStudent(ctx, req)
```

The important detail is:

```text
The server does not dispatch based on the client type.
It dispatches based on FullMethodName.
```

A server-side request path is:

```text
comment-service receives HTTP/2 + protobuf request
 -> gRPC reads FullMethodName
 -> finds ServiceDesc and MethodDesc
 -> enters generated handler
 -> handler decodes request
 -> Kratos middleware runs
 -> handler calls our StudentService method
 -> biz
 -> data
 -> MySQL / Redis / Elasticsearch
```

## 4. BFF Remote Call: Generated Client Stub

The BFF is the client side of the gRPC call.

`comment-student` keeps its own copy of the protocol SDK:

```text
comment-student/api/comment/v1/comment.proto
comment-student/api/comment/v1/comment.pb.go
comment-student/api/comment/v1/comment_grpc.pb.go
```

This means:

```text
comment-student does not need to depend on the comment-service module.
It only needs the same proto contract and generated client code.
```

The BFF creates a generated client:

```go
studentClient := commentv1.NewStudentServiceClient(conn)
```

The generated client stub looks like this:

```go
func (c *studentServiceClient) GetPostDetailStudent(
    ctx context.Context,
    in *GetPostDetailRequest,
    opts ...grpc.CallOption,
) (*GetPostDetailReply, error) {
    out := new(GetPostDetailReply)

    err := c.cc.Invoke(
        ctx,
        StudentService_GetPostDetailStudent_FullMethodName,
        in,
        out,
        opts...,
    )

    if err != nil {
        return nil, err
    }
    return out, nil
}
```

This code is not for the server to directly call methods. It is client-side stub code.

Its purpose is:

```text
Let the client call a remote RPC as if it were a local Go method.
```

The BFF only writes:

```go
reply, err := studentClient.GetPostDetailStudent(ctx, req)
```

But internally this happens:

```text
1. Create response object out
2. Use FullMethodName:
   /api.comment.v1.StudentService/GetPostDetailStudent
3. Call c.cc.Invoke(...)
4. gRPC serializes request as protobuf
5. gRPC sends it through HTTP/2
6. comment-service handles the request
7. gRPC receives the response
8. gRPC deserializes the response into out
9. The stub returns out
```

The `cc` field is:

```go
type studentServiceClient struct {
    cc grpc.ClientConnInterface
}
```

`cc` is the client-side connection abstraction. It is not a local server object.

So this:

```go
studentClient.GetPostDetailStudent(ctx, req)
```

is actually:

```text
studentClient.GetPostDetailStudent(...)
 -> c.cc.Invoke(...)
 -> network request
 -> comment-service gRPC Server
 -> generated handler
 -> our StudentService.GetPostDetailStudent(...)
```

## 5. Student BFF Request Flow

Example: student views post detail.

```text
1. Frontend calls comment-student:
   GET /api/student/posts/1001
   Header: x-user-id: 2001

2. BFF AuthFilter parses student identity:
   student_id = 2001

3. BFF handler builds gRPC request:
   GetPostDetailRequest{
     PostId: 1001,
     UserId: 2001,
     CommentPageNum: 1,
     CommentPageSize: 20,
   }

4. BFF calls generated client:
   studentClient.GetPostDetailStudent(ctx, req)

5. Client stub calls:
   cc.Invoke(
     "/api.comment.v1.StudentService/GetPostDetailStudent",
     req,
     out,
   )

6. gRPC sends HTTP/2 + protobuf to comment-service

7. comment-service gRPC Server dispatches by FullMethodName

8. generated handler calls:
   studentService.GetPostDetailStudent(ctx, req)

9. comment-service continues:
   service -> biz -> data -> Redis/MySQL

10. Response returns to BFF and then frontend
```

The condensed chain is:

```text
BFF handler
 -> generated client stub
 -> ClientConn.Invoke(fullMethod, req, resp)
 -> HTTP/2 + protobuf
 -> comment-service gRPC Server
 -> ServiceDesc matches handler
 -> Kratos middleware
 -> our service method
 -> biz/data
```

## 6. Wire Dependency Injection

Wire is used for compile-time dependency injection. It does not participate in each request.

It generates code that constructs objects in the correct order.

For `comment-student`, the construction chain is:

```text
NewConsulRegistry
 -> NewRegistrar
 -> NewDiscovery
 -> NewCommentClient
 -> NewStudentCommentService
 -> NewHTTPServer
 -> newApp
```

This means:

```text
config
 -> Consul registry/discovery
 -> comment-service gRPC client
 -> BFF handler
 -> HTTP Server
 -> Kratos App
```

Wire solves:

```text
who depends on whom
how objects are constructed at startup
how cleanup functions are returned
```

It is not a runtime container.

## 7. Service Registration vs Service Discovery

There are two different meanings of "registration".

### 7.1 RPC Method Registration

```go
RegisterStudentServiceServer(srv, studentService)
```

This registers:

```text
generated ServiceDesc + our service implementation
```

It solves:

```text
When a request arrives, which method should the gRPC Server call?
```

### 7.2 Service Instance Registration

```go
kratos.Registrar(registrar)
```

This registers the running service instance into Consul.

It solves:

```text
Where can other services find this service?
```

### 7.3 Discovery

BFF uses discovery to find `comment-service`:

```go
grpc.WithDiscovery(discovery)
grpc.WithEndpoint("discovery:///comment-service")
```

It solves:

```text
How does the caller find remote service instances?
```

Important distinction:

```text
RPC method registration:
  solves "which method should handle this request?"

Service instance registration:
  solves "where is this service running?"

Discovery:
  solves "how does the caller find the remote service?"
```

Short interview sentence:

```text
Registrar means I tell the registry where I am.
Discovery means I ask the registry where another service is.
```

## 8. BFF Authentication Boundary

The BFF is a good place for:

```text
login/session validation
role validation
extracting studentId from token
injecting studentId into gRPC request
preventing frontend from forging student_id
```

For example:

```go
StudentId: p.UserID
```

instead of:

```go
StudentId: req.StudentId
```

The BFF proves:

```text
this request belongs to student 2001
```

But resource ownership still belongs in `comment-service`.

Example:

```text
BFF checks the user is student 2001
comment-service checks whether comment_id belongs to student 2001
```

The boundary is:

```text
BFF:
  authentication and role-level authorization

comment-service:
  business authorization and resource ownership checks
```

## 9. Complete Mental Model

The complete Kratos + BFF chain is:

```text
proto defines service contract
 -> protoc generates pb/go-grpc/http/validate code
 -> service implements generated server interface
 -> RegisterXXXServer registers implementation into gRPC Server
 -> Wire constructs repo/usecase/service/server/app
 -> Kratos App starts HTTP/gRPC Server
 -> Registrar registers service instance into Consul
 -> BFF holds client SDK generated from the same proto
 -> BFF discovers comment-service through Consul
 -> BFF calls generated client method
 -> client stub calls ClientConn.Invoke(fullMethod, req, resp)
 -> gRPC sends HTTP/2 + protobuf
 -> comment-service generated handler dispatches to service method
 -> service -> biz -> data
```

## 10. Interview Explanation

You can explain it like this:

```text
Kratos is a proto-first microservice framework. We first define service methods,
request types, reply types, HTTP mappings, and validation rules in proto. The
generated gRPC code provides both client-side stubs and server-side interfaces,
handlers, and Register functions.

On the server side, my StudentService implements the generated
StudentServiceServer interface. During startup, RegisterStudentServiceServer
registers the generated ServiceDesc and my service implementation into the
gRPC Server. When a request comes in, gRPC dispatches by FullMethodName, enters
the generated handler, runs Kratos middleware, and finally calls my service
method.

On the BFF side, comment-student holds client code generated from the same proto.
It exposes HTTP APIs to the frontend, authenticates the student, injects
studentId into the gRPC request, discovers comment-service through Consul, and
calls the generated client method. Under the hood, the client stub calls
ClientConn.Invoke(fullMethod, req, resp), and gRPC handles protobuf
serialization, HTTP/2 transport, and response deserialization.

There are two kinds of registration. RegisterXXXServer is internal RPC method
registration, solving which method handles a request. kratos.Registrar is
service instance registration, solving where callers can find this service.
```

