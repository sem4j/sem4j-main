REST_URL = 'http://localhost:7474/'
HEADER = { 'Content-Type' => 'application/json' }

%w{rubygems json cgi faraday}.each{|r| require r}

# make a connection to the Neo4j REST server
conn = Faraday.new(:url => REST_URL) do |builder|
  builder.adapter :net_http
end

puts "begin processing..."

$delete_n = 0
$delete_r = 0
file_name = ARGV[0]

# RELATIONSHIPS (THIS ORDER IS IMPORTANT!)
res = conn.get("/db/data/index/relationship/files?query=file:#{file_name}")
array = JSON.parse(res.body)
for hash in array do
  #puts "\nDelete Relationship: ", hash["self"]
  _, id = hash["self"].split(/([^\/]+)$/)
  res2 = conn.delete("/db/data/relationship/#{id}")
  #puts "/db/data/relationsip/#{id}", res2.status
  $delete_r += 1
end

# NODES
res = conn.get("/db/data/index/node/files?query=file:#{file_name}")
array = JSON.parse(res.body)
for hash in array do
  #puts "\nDelete Node: ", hash["self"]
  _, id = hash["self"].split(/([^\/]+)$/)
  res2 = conn.delete("/db/data/node/#{id}")
  #puts "/db/data/node/#{node_id}", res2.status
  $delete_n += 1
end

puts "  #{$delete_n} nodes deleted"
puts "  #{$delete_r} relationships deleted"

puts "done!"
