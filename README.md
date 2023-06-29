## Notes

```
$ grpcurl --plaintext --proto ./api/pb/api.proto localhost:9900 pb.Service.Put 
ERROR:
  Code: Unknown
  Message: DUMMY ERROR

$ grpcurl --plaintext \
  -d '{"key": "plop", "value": "plip", "force": true}' \
  --proto ./api/pb/api.proto \
  localhost:9900 pb.Service.Put

# with reflection, no need to pass proto file
$ grpcurl -d '{"key": "plop", "value": "plip", "force": true}' --plaintext localhost:9900 pb.Service.Put
{
  "success": true
}
```
