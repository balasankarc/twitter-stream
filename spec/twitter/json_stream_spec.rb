require 'spec_helper.rb'
require 'twitter/json_stream'

include Twitter

Host = "127.0.0.1"
Port = 9550

class JSONServer < EM::Connection
  attr_accessor :data
  def receive_data data
    $recieved_data = data
    send_data $data_to_send
    EventMachine.next_tick {
      close_connection if $close_connection
    }
  end
end



describe JSONStream do

  context "authentication" do
    it "should connect with basic auth credentials" do
      connect_stream :auth => "username:password", :ssl => false
      expect($recieved_data).to include('Authorization: Basic')
    end

    it "should connect with oauth credentials" do
      oauth = {
        :consumer_key => '1234567890',
        :consumer_secret => 'abcdefghijklmnopqrstuvwxyz',
        :access_key => 'ohai',
        :access_secret => 'ohno'
      }
      connect_stream :oauth => oauth, :ssl => false
      expect($recieved_data).to include('Authorization: OAuth')
    end
  end

  context "on create" do

    it "should return stream" do
      expect(EM).to receive(:connect).and_return('TEST INSTANCE')
      stream = JSONStream.connect {}
      expect(stream).to eq('TEST INSTANCE')
    end

    it "should define default properties" do
      expect(EM).to receive(:connect).with(any_args) { |host, port, handler, opts|
        expect(host).to eq('stream.twitter.com')
        expect(port).to eq(443)
        expect(opts[:path]).to eq('/1/statuses/filter.json')
        expect(opts[:method]).to eq('GET')
      }
      stream = JSONStream.connect {}
    end

    it "should connect to the proxy if provided" do
      expect(EM).to receive(:connect).with(any_args) { |host, port, handler, opts|
        expect(host).to eq('my-proxy')
        expect(port).to eq(8080)
        expect(opts[:host]).to eq('stream.twitter.com')
        expect(opts[:port]).to eq(443)
        expect(opts[:proxy]).to eq('http://my-proxy:8080')
      }
      stream = JSONStream.connect(:proxy => "http://my-proxy:8080") {}
    end

    it "should not trigger SSL until connection is established" do
      connection = double('connection')
      expect(EM).to receive(:connect).and_return(connection)
      stream = JSONStream.connect(:ssl => true)
      expect(stream).to eq(connection)
    end

  end

  context "on valid stream" do
    attr_reader :stream
    before :each do
      $body = File.readlines(fixture_path("twitter/tweets.txt"))
      $body.each {|tweet| tweet.strip!; tweet << "\r" }
      $data_to_send = http_response(200, "OK", {}, $body)
      $recieved_data = ''
      $close_connection = false
    end

    it "should add no params" do
      connect_stream :ssl => false
      expect($recieved_data).to include('/1/statuses/filter.json HTTP')
    end

    it "should add custom params" do
      connect_stream :params => {:name => 'test'},  :ssl => false
      expect($recieved_data).to include('?name=test')
    end

    it "should parse headers" do
      connect_stream :ssl => false
      expect(stream.code).to eq(200)
      expect(stream.headers.keys.map{|k| k.downcase}).to include('content-type')
    end

    it "should parse headers even after connection close" do
      connect_stream :ssl => false
      expect(stream.code).to eq(200)
      expect(stream.headers.keys.map{|k| k.downcase}).to include('content-type')
    end

    it "should extract records" do
      connect_stream :user_agent => 'TEST_USER_AGENT',  :ssl => false
      expect($recieved_data.upcase).to include('USER-AGENT: TEST_USER_AGENT')
    end

    it 'should allow custom headers' do
      connect_stream :headers => { 'From' => 'twitter-stream' },  :ssl => false
      expect($recieved_data.upcase).to include('FROM: TWITTER-STREAM')
    end

    it "should deliver each item" do
      items = []
      connect_stream :ssl => false do
        stream.each_item do |item|
          items << item
        end
      end
      # Extract only the tweets from the fixture
      tweets = $body.map{|l| l.strip }.select{|l| l =~ /^\{/ }
      expect(items.size).to eq(tweets.size)
      tweets.each_with_index do |tweet,i|
        expect(items[i]).to eq(tweet)
      end
    end

    it "should swallow StandardError exceptions when delivering items" do
      expect do
        connect_stream :ssl => false do
          stream.each_item { |item| raise StandardError, 'error message' }
        end
      end.to_not raise_error
    end


    it "propagates out runtime errors when delivering items" do
      expect do
        connect_stream :ssl => false do
          stream.each_item { |item| raise Exception, 'error message' }
        end
      end.to raise_error(Exception, 'error message')
    end

    it "should send correct user agent" do
      connect_stream
    end
  end

  shared_examples_for "network failure" do
    it "should reconnect on network failure" do
      connect_stream do
        expect(stream).to receive(:reconnect)
      end
    end

    it "should not reconnect on network failure when not configured to auto reconnect" do
      connect_stream(:auto_reconnect => false) do
        expect(stream).to receive(:reconnect).never
      end
    end

    it "should reconnect with 0.25 at base" do
      connect_stream do
        expect(stream).to receive(:reconnect_after).with(0.25)
      end
    end

    it "should reconnect with linear timeout" do
      connect_stream do
        stream.nf_last_reconnect = 1
        expect(stream).to receive(:reconnect_after).with(1.25)
      end
    end

    it "should stop reconnecting after 100 times" do
      connect_stream do
        stream.reconnect_retries = 100
        expect(stream).not_to receive(:reconnect_after)
      end
    end

    it "should notify after reconnect limit is reached" do
      timeout, retries = nil, nil
      connect_stream do
        stream.on_max_reconnects do |t, r|
          timeout, retries = t, r
        end
        stream.reconnect_retries = 100
      end
      expect(timeout).to eq(0.25)
      expect(retries).to eq(101)
    end
  end

  context "on network failure" do
    attr_reader :stream
    before :each do
      $data_to_send = ''
      $close_connection = true
    end

    it "should timeout on inactivity" do
      connect_stream :stop_in => 1.5 do
        expect(stream).to receive(:reconnect)
      end
    end

    it "should not reconnect on inactivity when not configured to auto reconnect" do
      connect_stream(:stop_in => 1.5, :auto_reconnect => false) do
        expect(stream).to receive(:reconnect).never
      end
    end

    it "should reconnect with SSL if enabled" do
      connect_stream :ssl => true do
        expect(stream).to receive(:start_tls).twice
      end
    end

    it_should_behave_like "network failure"
  end

  context "on no data received" do
    attr_reader :stream
    before :each do
      $data_to_send = ''
      $close_connection = false
    end

    it "should call no data callback after no data received for 90 seconds" do
      connect_stream :stop_in => 6 do
        stream.last_data_received_at = Time.now - 88
        expect(stream).to receive(:no_data).once
      end
    end

  end

  context "on server unavailable" do

    attr_reader :stream

    # This is to make it so the network failure specs which call connect_stream
    # can be reused. This way calls to connect_stream won't actually create a
    # server to listen in.
    def connect_stream_without_server(opts={},&block)
      connect_stream_default(opts.merge(:start_server=>false),&block)
    end
    alias_method :connect_stream_default, :connect_stream
    alias_method :connect_stream, :connect_stream_without_server

    it_should_behave_like "network failure"
  end

  context "on application failure" do
    attr_reader :stream
    before :each do
      $data_to_send = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"Firehose\"\r\n\r\n"
      $close_connection = false
    end

    it "should reconnect on application failure 10 at base" do
      connect_stream :ssl => false do
        expect(stream).to receive(:reconnect_after).with(10)
      end
    end

    it "should not reconnect on application failure 10 at base when not configured to auto reconnect" do
      connect_stream  :ssl => false, :auto_reconnect => false do
        expect(stream).to receive(:reconnect_after).never
      end
    end

    it "should reconnect with exponential timeout" do
      connect_stream :ssl => false do
        stream.af_last_reconnect = 160
        expect(stream).to receive(:reconnect_after).with(320)
      end
    end

    it "should not try to reconnect after limit is reached" do
      connect_stream :ssl => false do
        stream.af_last_reconnect = 320
        expect(stream).not_to receive(:reconnect_after)
      end
    end
  end

  context "on stream with chunked transfer encoding" do
    attr_reader :stream
    before :each do
      $recieved_data = ''
      $close_connection = false
    end

    it "should ignore empty lines" do
      body_chunks = ["{\"screen"+"_name\"",":\"user1\"}\r\r\r{","\"id\":9876}\r\r"]
      $data_to_send = http_response(200,"OK",{},body_chunks)
      items = []
      connect_stream :ssl => false do
        stream.each_item do |item|
          items << item
        end
      end
      expect(items.size).to eq(2)
      expect(items[0]).to eq('{"screen_name":"user1"}')
      expect(items[1]).to eq('{"id":9876}')
    end

    it "should parse full entities even if split" do
      body_chunks = ["{\"id\"",":1234}\r{","\"id\":9876}"]
      $data_to_send = http_response(200,"OK",{},body_chunks)
      items = []
      connect_stream :ssl => false do
        stream.each_item do |item|
          items << item
        end
      end
      expect(items.size).to eq(2)
      expect(items[0]).to eq('{"id":1234}')
      expect(items[1]).to eq('{"id":9876}')
    end
  end

end
