package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type HLC struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Ts            int64                  `protobuf:"varint,1,opt,name=ts,proto3" json:"ts,omitempty"`
	Seq           int64                  `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *HLC) Reset() {
	*x = HLC{}
	mi := &file_v1_sync_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *HLC) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HLC) ProtoMessage() {}

func (x *HLC) ProtoReflect() protoreflect.Message {
	mi := &file_v1_sync_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*HLC) Descriptor() ([]byte, []int) {
	return file_v1_sync_proto_rawDescGZIP(), []int{0}
}

func (x *HLC) GetTs() int64 {
	if x != nil {
		return x.Ts
	}
	return 0
}

func (x *HLC) GetSeq() int64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

type Row struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Kind          string                 `protobuf:"bytes,1,opt,name=kind,proto3" json:"kind,omitempty"`
	Key           string                 `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Owner         string                 `protobuf:"bytes,3,opt,name=owner,proto3" json:"owner,omitempty"`
	Hlc           *HLC                   `protobuf:"bytes,4,opt,name=hlc,proto3" json:"hlc,omitempty"`
	Tombstone     bool                   `protobuf:"varint,5,opt,name=tombstone,proto3" json:"tombstone,omitempty"`
	Payload       []byte                 `protobuf:"bytes,6,opt,name=payload,proto3" json:"payload,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Row) Reset() {
	*x = Row{}
	mi := &file_v1_sync_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Row) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Row) ProtoMessage() {}

func (x *Row) ProtoReflect() protoreflect.Message {
	mi := &file_v1_sync_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Row) Descriptor() ([]byte, []int) {
	return file_v1_sync_proto_rawDescGZIP(), []int{1}
}

func (x *Row) GetKind() string {
	if x != nil {
		return x.Kind
	}
	return ""
}

func (x *Row) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Row) GetOwner() string {
	if x != nil {
		return x.Owner
	}
	return ""
}

func (x *Row) GetHlc() *HLC {
	if x != nil {
		return x.Hlc
	}
	return nil
}

func (x *Row) GetTombstone() bool {
	if x != nil {
		return x.Tombstone
	}
	return false
}

func (x *Row) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

type PullRequest struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	NodeId         string                 `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Watermarks     map[string]*HLC        `protobuf:"bytes,2,rep,name=watermarks,proto3" json:"watermarks,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Checksums      map[string]uint64      `protobuf:"bytes,3,rep,name=checksums,proto3" json:"checksums,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	EventWatermark *HLC                   `protobuf:"bytes,4,opt,name=event_watermark,json=eventWatermark,proto3" json:"event_watermark,omitempty"`
	EventChecksum  uint64                 `protobuf:"varint,5,opt,name=event_checksum,json=eventChecksum,proto3" json:"event_checksum,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *PullRequest) Reset() {
	*x = PullRequest{}
	mi := &file_v1_sync_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PullRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PullRequest) ProtoMessage() {}

func (x *PullRequest) ProtoReflect() protoreflect.Message {
	mi := &file_v1_sync_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PullRequest) Descriptor() ([]byte, []int) {
	return file_v1_sync_proto_rawDescGZIP(), []int{2}
}

func (x *PullRequest) GetNodeId() string {
	if x != nil {
		return x.NodeId
	}
	return ""
}

func (x *PullRequest) GetWatermarks() map[string]*HLC {
	if x != nil {
		return x.Watermarks
	}
	return nil
}

func (x *PullRequest) GetChecksums() map[string]uint64 {
	if x != nil {
		return x.Checksums
	}
	return nil
}

func (x *PullRequest) GetEventWatermark() *HLC {
	if x != nil {
		return x.EventWatermark
	}
	return nil
}

func (x *PullRequest) GetEventChecksum() uint64 {
	if x != nil {
		return x.EventChecksum
	}
	return 0
}

type Event struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	RunnerId      string                 `protobuf:"bytes,1,opt,name=runner_id,json=runnerId,proto3" json:"runner_id,omitempty"`
	Seq           int64                  `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	Type          string                 `protobuf:"bytes,3,opt,name=type,proto3" json:"type,omitempty"`
	Payload       string                 `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	Hlc           *HLC                   `protobuf:"bytes,5,opt,name=hlc,proto3" json:"hlc,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Event) Reset() {
	*x = Event{}
	mi := &file_v1_sync_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Event) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Event) ProtoMessage() {}

func (x *Event) ProtoReflect() protoreflect.Message {
	mi := &file_v1_sync_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Event) Descriptor() ([]byte, []int) {
	return file_v1_sync_proto_rawDescGZIP(), []int{3}
}

func (x *Event) GetRunnerId() string {
	if x != nil {
		return x.RunnerId
	}
	return ""
}

func (x *Event) GetSeq() int64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *Event) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Event) GetPayload() string {
	if x != nil {
		return x.Payload
	}
	return ""
}

func (x *Event) GetHlc() *HLC {
	if x != nil {
		return x.Hlc
	}
	return nil
}

type PullResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	NodeId        string                 `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Rows          []*Row                 `protobuf:"bytes,2,rep,name=rows,proto3" json:"rows,omitempty"`
	Checksums     map[string]uint64      `protobuf:"bytes,3,rep,name=checksums,proto3" json:"checksums,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	Events        []*Event               `protobuf:"bytes,4,rep,name=events,proto3" json:"events,omitempty"`
	EventChecksum uint64                 `protobuf:"varint,5,opt,name=event_checksum,json=eventChecksum,proto3" json:"event_checksum,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PullResponse) Reset() {
	*x = PullResponse{}
	mi := &file_v1_sync_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PullResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PullResponse) ProtoMessage() {}

func (x *PullResponse) ProtoReflect() protoreflect.Message {
	mi := &file_v1_sync_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PullResponse) Descriptor() ([]byte, []int) {
	return file_v1_sync_proto_rawDescGZIP(), []int{4}
}

func (x *PullResponse) GetNodeId() string {
	if x != nil {
		return x.NodeId
	}
	return ""
}

func (x *PullResponse) GetRows() []*Row {
	if x != nil {
		return x.Rows
	}
	return nil
}

func (x *PullResponse) GetChecksums() map[string]uint64 {
	if x != nil {
		return x.Checksums
	}
	return nil
}

func (x *PullResponse) GetEvents() []*Event {
	if x != nil {
		return x.Events
	}
	return nil
}

func (x *PullResponse) GetEventChecksum() uint64 {
	if x != nil {
		return x.EventChecksum
	}
	return 0
}

var File_v1_sync_proto protoreflect.FileDescriptor

const file_v1_sync_proto_rawDesc = "" +
	"\n" +
	"\rv1/sync.proto\x12\atakl.v1\"'\n" +
	"\x03HLC\x12\x0e\n" +
	"\x02ts\x18\x01 \x01(\x03R\x02ts\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x03R\x03seq\"\x99\x01\n" +
	"\x03Row\x12\x12\n" +
	"\x04kind\x18\x01 \x01(\tR\x04kind\x12\x10\n" +
	"\x03key\x18\x02 \x01(\tR\x03key\x12\x14\n" +
	"\x05owner\x18\x03 \x01(\tR\x05owner\x12\x1e\n" +
	"\x03hlc\x18\x04 \x01(\v2\f.takl.v1.HLCR\x03hlc\x12\x1c\n" +
	"\ttombstone\x18\x05 \x01(\bR\ttombstone\x12\x18\n" +
	"\apayload\x18\x06 \x01(\fR\apayload\"\x98\x03\n" +
	"\vPullRequest\x12\x17\n" +
	"\anode_id\x18\x01 \x01(\tR\x06nodeId\x12D\n" +
	"\n" +
	"watermarks\x18\x02 \x03(\v2$.takl.v1.PullRequest.WatermarksEntryR\n" +
	"watermarks\x12A\n" +
	"\tchecksums\x18\x03 \x03(\v2#.takl.v1.PullRequest.ChecksumsEntryR\tchecksums\x125\n" +
	"\x0fevent_watermark\x18\x04 \x01(\v2\f.takl.v1.HLCR\x0eeventWatermark\x12%\n" +
	"\x0eevent_checksum\x18\x05 \x01(\x04R\reventChecksum\x1aK\n" +
	"\x0fWatermarksEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\"\n" +
	"\x05value\x18\x02 \x01(\v2\f.takl.v1.HLCR\x05value:\x028\x01\x1a<\n" +
	"\x0eChecksumsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x04R\x05value:\x028\x01\"\x84\x01\n" +
	"\x05Event\x12\x1b\n" +
	"\trunner_id\x18\x01 \x01(\tR\brunnerId\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x03R\x03seq\x12\x12\n" +
	"\x04type\x18\x03 \x01(\tR\x04type\x12\x18\n" +
	"\apayload\x18\x04 \x01(\tR\apayload\x12\x1e\n" +
	"\x03hlc\x18\x05 \x01(\v2\f.takl.v1.HLCR\x03hlc\"\x9a\x02\n" +
	"\fPullResponse\x12\x17\n" +
	"\anode_id\x18\x01 \x01(\tR\x06nodeId\x12 \n" +
	"\x04rows\x18\x02 \x03(\v2\f.takl.v1.RowR\x04rows\x12B\n" +
	"\tchecksums\x18\x03 \x03(\v2$.takl.v1.PullResponse.ChecksumsEntryR\tchecksums\x12&\n" +
	"\x06events\x18\x04 \x03(\v2\x0e.takl.v1.EventR\x06events\x12%\n" +
	"\x0eevent_checksum\x18\x05 \x01(\x04R\reventChecksum\x1a<\n" +
	"\x0eChecksumsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x04R\x05value:\x028\x012B\n" +
	"\vSyncService\x123\n" +
	"\x04Pull\x12\x14.takl.v1.PullRequest\x1a\x15.takl.v1.PullResponseB\x1fZ\x1dtakl/internal/transport/pb;pbb\x06proto3"

var (
	file_v1_sync_proto_rawDescOnce sync.Once
	file_v1_sync_proto_rawDescData []byte
)

func file_v1_sync_proto_rawDescGZIP() []byte {
	file_v1_sync_proto_rawDescOnce.Do(func() {
		file_v1_sync_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_v1_sync_proto_rawDesc), len(file_v1_sync_proto_rawDesc)))
	})
	return file_v1_sync_proto_rawDescData
}

var file_v1_sync_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_v1_sync_proto_goTypes = []any{
	(*HLC)(nil),
	(*Row)(nil),
	(*PullRequest)(nil),
	(*Event)(nil),
	(*PullResponse)(nil),
	nil,
	nil,
	nil,
}
var file_v1_sync_proto_depIdxs = []int32{
	0,
	5,
	6,
	0,
	0,
	1,
	7,
	3,
	0,
	2,
	4,
	10,
	9,
	9,
	9,
	0,
}

func init() { file_v1_sync_proto_init() }
func file_v1_sync_proto_init() {
	if File_v1_sync_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_v1_sync_proto_rawDesc), len(file_v1_sync_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_v1_sync_proto_goTypes,
		DependencyIndexes: file_v1_sync_proto_depIdxs,
		MessageInfos:      file_v1_sync_proto_msgTypes,
	}.Build()
	File_v1_sync_proto = out.File
	file_v1_sync_proto_goTypes = nil
	file_v1_sync_proto_depIdxs = nil
}
