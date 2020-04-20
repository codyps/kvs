@0xfd588caf46f4202e;

struct Entry {
	union {
		set @0 :Set;
		rm @1: Rm;
	}

	struct Set {
		key @0 :Text;
		value @1 :Text;
	}
	
	struct Rm {
		key @0 :Text;
	}
}
