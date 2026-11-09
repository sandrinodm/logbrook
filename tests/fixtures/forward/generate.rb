require 'msgpack'
require 'json'
require 'zlib'
require 'stringio'
time = MessagePack::ExtensionValue.new(0, [1700000000, 123456789].pack('NN'))
record = {'container_id' => 'a' * 64, 'container_name' => '/fixture-api', 'source' => 'stdout', 'log' => '{"level":30,"msg":"reference event"}'}
entries = [[time, record], [1700000001, {'message' => 'second reference event'}]]
packed = entries.map(&:to_msgpack).join
legacy_str = [0xdb, packed.bytesize].pack('CN') + packed
prefix = [0x93].pack('C') + 'fixture.api'.to_msgpack
opts = {'chunk' => 'fixture-chunk'}.to_msgpack
def gzip(bytes)
 io = StringIO.new
 writer = Zlib::GzipWriter.new(io)
 writer.mtime = 0
 writer.write(bytes)
 writer.close
 io.string
end
fixtures = {
 'message_eventtime' => ['fixture.api', time, record, {'chunk' => 'fixture-chunk'}].to_msgpack,
 'forward' => ['fixture.api', entries, {'chunk' => 'fixture-chunk'}].to_msgpack,
 'packed_bin' => ['fixture.api', packed.b, {'chunk' => 'fixture-chunk'}].to_msgpack,
 'packed_str' => prefix + legacy_str + opts,
 'gzip_multimember' => ['fixture.api', (gzip(entries[0].to_msgpack) + gzip(entries[1].to_msgpack)).b, {'compressed' => 'gzip', 'chunk' => 'fixture-chunk'}].to_msgpack
}
puts JSON.generate({'ruby' => RUBY_VERSION, 'msgpack' => MessagePack::VERSION, 'fixtures' => fixtures.transform_values { |v| v.unpack1('H*') }})
