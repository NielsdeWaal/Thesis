@0xbb15653a0870b80a;

struct Tag {
	name @0 :Text;
	value @1 :Text;
}

struct Batch {
	struct Message {
		metric @0 :Text;
		tags @1 :List(Tag);
		measurements @2 :List(Measurement);
		timestamp @3 :UInt64;

		struct Measurement {
			name @0 :Text;
			value @1 :UInt64;
		}
	}

	recordings @0 :List(Message);
}

struct IdRequest {
	tagSet @0 :List(Tag);
	metric @1 :Text;
	identifier @2 :UInt64;
}

struct IdResponse {
	setId @0 :UInt64;
	identifier @1 :UInt64;
}

struct InsertionBatch {
	struct Message {
		tag @0 :UInt64;
		measurements @1 :List(Measurement);

		struct Measurement {
			timestamp @0 :UInt64;
			value @1 :Int64;
		}
	}
	
	recordings @0 :List(Message);
}

struct InsertionResponse {
	
}