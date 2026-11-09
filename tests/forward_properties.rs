//! Shrinking properties supplement independent sender fixtures and libFuzzer.
use base64::{Engine, engine::general_purpose::STANDARD};
use logbrook::forward::{
    codec::Framer,
    decode::{decode_frame, decode_value, encode_value},
    normalize::{NormalizationOptions, normalize_envelope},
    protocol::{CodecLimits, DecodedFrame, Value},
};
use proptest::prelude::*;

fn value_strategy() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        Just(Value::Nil),
        any::<bool>().prop_map(Value::Boolean),
        any::<i64>().prop_map(|n| Value::Integer(i128::from(n))),
        any::<u64>().prop_map(|n| Value::Integer(i128::from(n))),
        any::<u32>().prop_map(Value::Float32),
        any::<u64>().prop_map(Value::Float64),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(Value::String),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(Value::Binary),
        (any::<i8>(), prop::collection::vec(any::<u8>(), 0..32))
            .prop_map(|(kind, bytes)| Value::Extension(kind, bytes)),
    ];
    leaf.prop_recursive(4, 64, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((inner.clone(), inner), 0..4).prop_map(Value::Map),
        ]
    })
    .boxed()
}

fn record_strategy() -> BoxedStrategy<Value> {
    prop::collection::vec(
        (prop::collection::vec(any::<u8>(), 0..12), value_strategy()),
        0..6,
    )
    .prop_map(|fields| {
        Value::Map(
            fields
                .into_iter()
                .map(|(key, value)| (Value::String(key), value))
                .collect(),
        )
    })
    .boxed()
}

fn message(record: &Value) -> Vec<u8> {
    encode_value(&Value::Array(vec![
        Value::string("property.tag"),
        Value::Integer(1),
        record.clone(),
    ]))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn recursive_messagepack_round_trip_preserves_order_types_and_float_bits(
        value in value_strategy()
    ) {
        let wire = encode_value(&value);
        prop_assert_eq!(decode_value(&wire, &CodecLimits::default()).unwrap(), value);
}

    #[test]
    fn arbitrary_fragmentation_and_coalescing_preserves_envelope_boundaries(
        record in record_strategy(),
        chunk_sizes in prop::collection::vec(1usize..128, 1..16),
    ) {
        let wire = message(&record);
        let mut coalesced = wire.clone();
        coalesced.push(0xc0);
        coalesced.extend_from_slice(&wire);

        let mut framer = Framer::new(CodecLimits::default());
        let mut frames = Vec::new();
        let mut offset = 0usize;
        let mut chunks = chunk_sizes.iter().cycle();
        while offset < coalesced.len() {
            let end = (offset + chunks.next().unwrap()).min(coalesced.len());
            let chunk = &coalesced[offset..end];
            let capacity = framer.capacity_after_push(chunk.len());
            framer.push(chunk).unwrap();
            prop_assert!(framer.allocated_bytes() <= capacity);

            while let Some(frame) = framer.next_frame().unwrap() {
                frames.push(frame);
            }
            offset = end;
        }

        prop_assert_eq!(frames, vec![wire.clone(), vec![0xc0], wire]);
        prop_assert_eq!(framer.buffered_len(), 0);
        prop_assert_eq!(framer.allocated_bytes(), 0);
}

    #[test]
    fn normalization_is_deterministic_and_fallback_reconstructs_exact_record(
        record in record_strategy()
    ) {
        let wire = message(&record);
        let DecodedFrame::Envelope(envelope) = decode_frame(&wire, &CodecLimits::default()).unwrap()
        else {
            return Err(TestCaseError::fail("expected envelope"));
        };

        let options = NormalizationOptions::default();
        let first = normalize_envelope(&envelope, &options).unwrap();
        let second = normalize_envelope(&envelope, &options).unwrap();

        prop_assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        prop_assert_eq!(first.len(), 1);
        prop_assert_eq!(&envelope.events[0].original_record, &encode_value(&record));

        if let Some(encoded) = first[0].attributes["forward"].get("original_record_msgpack_b64") {
            let preserved = STANDARD.decode(encoded.as_str().unwrap()).unwrap();
            prop_assert_eq!(&preserved, &envelope.events[0].original_record);
            prop_assert_eq!(
                decode_value(&preserved, &CodecLimits::default()).unwrap(),
                record
            );
        }
}

    #[test]
    fn malformed_final_record_never_returns_a_partially_validated_envelope(
        records in prop::collection::vec(record_strategy(), 0..8),
    ) {
        let mut entries: Vec<_> = records
            .into_iter()
            .map(|record| Value::Array(vec![Value::Integer(1), record]))
            .collect();

        entries.push(Value::Array(vec![
            Value::Integer(1),
            Value::Map(vec![(Value::Integer(1), Value::Nil)]),
        ]));

        let wire = encode_value(&Value::Array(vec![
            Value::string("property.tag"),
            Value::Array(entries),
        ]));
        prop_assert!(decode_frame(&wire, &CodecLimits::default()).is_err());
}

    #[test]
    fn arbitrary_untrusted_bytes_respect_small_decoder_caps(
        bytes in prop::collection::vec(any::<u8>(), 0..2048)
    ) {
        let limits = CodecLimits {
            max_frame_bytes: 2048,
            max_expanded_bytes: 4096,
            max_events: 8,
            max_event_bytes: 1024,
            max_depth: 8,
            max_values: 256,
            max_string_bytes: 4096,
            max_decoded_bytes: 32 * 1024,
            max_gzip_members: 2,
            ..CodecLimits::default()
        };
        if let Ok(DecodedFrame::Envelope(envelope)) = decode_frame(&bytes, &limits) {
            prop_assert!(envelope.events.len() <= limits.max_events);
            prop_assert!(
                envelope
                    .events
                    .iter()
                    .all(|event| event.original_record.len() <= limits.max_event_bytes)
            );
        }
}
}
