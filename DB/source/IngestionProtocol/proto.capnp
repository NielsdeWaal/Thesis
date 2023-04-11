@0xbb15653a0870b80a;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("proto");

struct Batch {
	struct Message {
		metric @0 :Text;
		tags @1 :List(Tag);
		measurements @2 :List(Measurement);
		timestamp @3 :UInt64;

		struct Tag {
			name @0 :Text;
			value @1 :Text;
		}

		struct Measurement {
			name @0 :Text;
			value @1 :UInt64;
		}
	}

	recordings @0 :List(Message);
}
