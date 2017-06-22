require 'thin'
require 'em-websocket'
require 'sinatra-websocket/error'
require 'sinatra-websocket/ext/thin/connection'
require 'sinatra-websocket/ext/sinatra/request'
require 'em-websocket/handshake'
require 'ostruct'

module SinatraWebsocket
  class Connection < ::EventMachine::WebSocket::Connection
    class << self
      def from_env(env, options = {})
        if env.include?('async.orig_callback')
          callback_key = 'async.orig_callback'
        elsif env.include?(Thin::Request::ASYNC_CALLBACK)
          callback_key = Thin::Request::ASYNC_CALLBACK
        else
          raise Error::ConfigurationError.new('Could not find an async callback in our environment!')
        end
        socket     = env[callback_key].receiver
        request    = request_from_env(env)
        connection = Connection.new(env, socket, :debug => options[:debug])
        yield(connection) if block_given?
        connection.dispatch(request) ? async_response : failure_response
      end

      # Parse Rack env to em-websocket-compatible format
      # this probably should be moved to Base in future
      def request_from_env(env)
        request = OpenStruct.new
        request.request_url = env['rack.url_scheme'] + '://' + env['HTTP_HOST'] + env['REQUEST_URI']
        request.http_method = env['REQUEST_METHOD']
        request['upgrade?'] = env['HTTP_CONNECTION'] && env['HTTP_UPGRADE'] &&
            env['HTTP_CONNECTION'].split(',').map(&:strip).map(&:downcase).include?('upgrade') &&
            env['HTTP_UPGRADE'].downcase == 'websocket'

        env.each do |key, value|
          if key.match(/HTTP_(.+)/)
            request[$1.downcase.gsub('_','-')] ||= value
          end
        end
        request
      end

      # Standard async response
      def async_response
        [-1, {}, []]
      end

      # Standard 400 response
      def failure_response
        [ 400, {'Content-Type' => 'text/plain'}, [ 'Bad request' ] ]
      end
    end # class << self


    #########################
    ### EventMachine part ###
    #########################

    # Overwrite new from EventMachine
    # we need to skip standard procedure called
    # when socket is created - this is just a stub
    def self.new(*args)
      instance = allocate
      instance.__send__(:initialize, *args)
      instance
    end

    # Overwrite send_data from EventMachine
    # delegate send_data to rack server
    def send_data(*args)
      EM.next_tick do
        @socket.send_data(*args)
      end
    end

    # Overwrite close_connection from EventMachine
    # delegate close_connection to rack server
    def close_connection(*args)
      EM.next_tick do
        @socket.close_connection(*args)
      end
    end

    #########################
    ### EM-WebSocket part ###
    #########################

    # Overwrite initialize from em-websocket
    # set all standard options and disable
    # EM connection inactivity timeout
    def initialize(app, socket, options = {})
      @app     = app
      @socket  = socket
      super(options)
      @ssl     = socket.backend.respond_to?(:ssl?) && socket.backend.ssl?

      socket.websocket = self
      socket.comm_inactivity_timeout = 0
    end

    def get_peername
      @socket.get_peername
    end

    # Overwrite dispath from em-websocket
    # we already have request headers parsed so
    # we must jerry rig the handshake
    def dispatch(data)
      return false if data.nil?
      @handshake ||= begin
        handshake = EventMachine::WebSocket::Handshake.new(@secure || @secure_proxy)

        handshake.callback { |upgrade_response, handler_klass|
          debug [:accepting_ws_version, handshake.protocol_version]
          debug [:upgrade_response, upgrade_response]
          self.send_data(upgrade_response)
          @handler = handler_klass.new(self, @debug)
          @handshake = nil
          trigger_on_open(handshake)
        }

        handshake.errback { |e|
          debug [:error, e]
          trigger_on_error(e)
          # Handshake errors require the connection to be aborted
          abort
        }

        handshake
      end

      @handshake.instance_eval {
        @parser = data
        @headers = data
        process(data, data)
      }
    end
  end
end # module::SinatraWebSocket
