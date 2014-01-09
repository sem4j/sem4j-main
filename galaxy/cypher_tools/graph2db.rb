REST_URL = ARGV[0]
HEADER = { 'Content-Type' => 'application/json' }

%w{rubygems json cgi faraday}.each{|r| require r}

# make a connection to the Neo4j REST server
conn = Faraday.new(:url => REST_URL) do |builder|
  builder.adapter :net_http
end

# GET OR CREATE NODE
def get_or_create_node(conn, name)
  # LOOK FOR THIS NODE IN THE INDEX
  r = conn.get("/db/data/index/node/#{$file_name}/name/#{CGI.escape(name)}")
  node = (JSON.parse(r.body).first || {})['self'] if r.status == 200
  #puts r.status, node
  unless node
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("/db/data/node", JSON.unparse({:name => name}), HEADER)
    node = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    # NAMESPACE INDEX
    data1 = {:uri => node, :key => "name", :value => CGI.escape(name)}
    conn.post("/db/data/index/node/#{$file_name}", JSON.unparse(data1), HEADER)
    # FILE INDEX
    data2 = {:uri => node, :key => "file", :value => $file_name}
    conn.post("/db/data/index/node/files", JSON.unparse(data2), HEADER)
    $load_n += 1
  else
    $skip_n += 1
  end
  # RETURN NEO4J'S NODE OBJECT
  node
end

# GET NODE
def get_node(conn, name)
  # LOOK FOR THIS NODE IN THE INDEX
  r = conn.get("/db/data/index/node/#{$file_name}/name/#{CGI.escape(name)}")
  node = (JSON.parse(r.body).first || {})['self'] if r.status == 200
  unless node
    puts "Error: get_node - No node found in this index"
  end
  node
end

# ADD A PROPERTY ON A NODE
def add_property(conn, node, value, type)
  # Check if the same property is already added
  r = conn.get("#{node}/properties/#{CGI.escape(type)}")
  #p r.status
  #puts r.body, value
  #puts JSON.parse(value_stored.body)['exception']
  if r.status != 200
    r = conn.put("#{node}/properties/#{type}", "#{value}", HEADER) 
    #p r.status
    #puts r.body
    $load_p += 1
  else
    if r.body == value
      $skip_p += 1
    else
      puts "WARNING (Property Conflict): #{node}/properties/#{CGI.escape(type)} old: #{r.body} <> new: #{value}"
      $warn_p += 1
    end
  end
  #puts "#{node}/properties/#{CGI.escape(type)}"
end

# method to get existing relationship using traversal from its start node, or create one
def get_or_create_relationship(conn, node_start, node_end, type)
  data = <<"EOS"
{
  "to" : "#{node_end}",
  "max_depth" : 1,
  "relationships" : {
    "type" : "#{type}",
    "direction" : "out"
  },
  "algorithm" : "shortestPath"
}
EOS
  r = conn.post("#{node_start}/paths", data, HEADER)
  length = (JSON.parse(r.body).first || {})['length'] if r.status == 200
  if length != 1
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("#{node_start}/relationships", JSON.unparse({ :to => node_end, :type => type }), HEADER)
    relationship = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    #puts r.status
    #puts relationship
    # FILE INDEX
    r_data = "{\"uri\" : \"#{relationship}\", \"key\" : \"file\", \"value\" : \"#{$file_name}\"}"
    r2 = conn.post("/db/data/index/relationship/files", r_data, HEADER)
    $load_r += 1
  else
    $skip_r += 1
  end
end


puts "begin processing..."

count = 0
$load_n = 0
$skip_n = 0
$load_r = 0
$skip_r = 0
$load_p = 0
$skip_p = 0
$warn_p = 0
$file_name = ""

File.open(ARGV[1]) do |io|
  _, $file_name = ARGV[1].split(/([^\/]+)$/)
  nodes = JSON.load(io)
  p "#{nodes.length} nodes" 
  for node in nodes do
    node_id = get_or_create_node(conn, node["id"])
    for property in node["properties"] do
      add_property(conn, node_id, property["value"], property["name"])
    end
  end
end

File.open(ARGV[2]) do |io|
  _, $file_name = ARGV[2].split(/([^\/]+)$/)
  rels = JSON.load(io)
  for rel in rels do
    source_id = get_node(conn, rel["source"])
    target_id = get_node(conn, rel["target"])
    rel_id = get_or_create_relationship(conn, source_id, target_id, rel["properties"][0]["value"])
  end
end

puts "  #{$load_n} nodes loaded"
puts "  #{$skip_n} nodes skipped"
puts "  #{$load_r} relationships loaded"
puts "  #{$skip_r} relationships skipped"
puts "  #{$load_p} properties loaded"
puts "  #{$skip_p} properties skipped"
puts "  #{$warn_p} properties warning"

puts "done!"
