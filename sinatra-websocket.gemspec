require File.expand_path('../lib/sinatra-websocket/version', __FILE__)

Gem::Specification.new do |s|
  s.name          = 'sinatra-websocket'
  s.version       = SinatraWebsocket::VERSION
  s.summary       = "Simple, upgradable WebSockets for Sinatra."
  s.description   = "Makes it easy to upgrade any request to a websocket connection in Sinatra"
  s.homepage      = 'http://github.com/simulacre/sinatra-websocket'
  s.email         = 'sinatra-websocket@simulacre.org'
  s.authors       = ['Caleb Crane']
  s.files         = Dir["lib/**/*.rb", "bin/*", "*.md"]
  s.require_paths = ["lib"]

  s.add_dependency 'eventmachine', '~>1.2.3'
  s.add_dependency 'thin', '>= 1.3.1', '<2.0.0'
  s.add_dependency 'em-websocket', '~>0.5.1'
  s.add_dependency 'sinatra', '~>1.4.8'
end
