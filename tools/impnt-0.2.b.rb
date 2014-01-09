REST_URL = 'http://localhost:7474/'
HEADER = { 'Content-Type' => 'application/json' }

%w{rubygems json cgi faraday}.each{|r| require r}

# make a connection to the Neo4j REST server
conn = Faraday.new(:url => REST_URL) do |builder|
  builder.adapter :net_http
end


# GET OR CREATE NODE
def get_or_create_node(conn, uri)
  # PARSE URI
  _, namespace, name = uri.split(/^(.*[\/|#])([^\/|#]+)$/)
  # LOOK FOR THIS NODE IN THE INDEX
  r = conn.get("/db/data/index/node/#{CGI.escape(namespace)}/name/#{CGI.escape(name)}")
  node = (JSON.parse(r.body).first || {})['self'] if r.status == 200
  #puts r.status, node
  unless node
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("/db/data/node", JSON.unparse({"name" => name, "uri" => uri}), HEADER)
    node = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    # NAMESPACE INDEX
    node_data = "{\"uri\" : \"#{node}\", \"key\" : \"name\", \"value\" : \"#{CGI.escape(name)}\"}"
    conn.post("/db/data/index/node/#{CGI.escape(namespace)}", node_data, HEADER)
    # FILE INDEX
    data2 = "{\"uri\" : \"#{node}\", \"key\" : \"file\", \"value\" : \"#{$file_name}\"}"
    r2 = conn.post("/db/data/index/node/files", data2, HEADER)
    # GET OR CREATE NAMESPACE NODE, AND CREATE RELATIONSHIP
    #namespace_node = get_or_create_namespace_node(conn, namespace)
    #get_or_create_relationship(conn, namespace_node, node, 'namespace_of')
    $load_n += 1
  else
    $skip_n += 1
  end
  # RETURN NEO4J'S NODE OBJECT
  node
end

def get_or_create_namespace_node(conn, name)
  # LOOK FOR THIS NODE IN THE INDEX
  r = conn.get("/db/data/index/node/namespaces/name/#{CGI.escape(name)}")
  node = (JSON.parse(r.body).first || {})['self'] if r.status == 200
  #puts r.status, node
  unless node
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("/db/data/node", JSON.unparse({"name" => name}), HEADER)
    node = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    # ADD NEW NODE TO THE INDEX
    node_data = "{\"uri\" : \"#{node}\", \"key\" : \"name\", \"value\" : \"#{CGI.escape(name)}\"}"
    conn.post("/db/data/index/node/namespaces", node_data, HEADER)
  else
  end
  node
end

# Method to add property on a node
def add_property(conn, node, value, type)
  # Check if the same property is already added
  r = conn.get("#{node}/properties/#{CGI.escape(type)}")
  #p r.status
  #p r.body, value
  #puts JSON.parse(value_stored.body)['exception']
  if r.status != 200
    conn.put("#{node}/properties/#{CGI.escape(type)}", "#{value.match(/"(.*)"/)}", HEADER) 
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
    puts r.status
    puts relationship
    # ADD THE NAME OF THE NEW NODE TO THE INDEX
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

File.open(ARGV[0]).each do |line|
  _, $file_name = ARGV[0].split(/([^\/]+)$/)
  uri_s, uri_p, uri_o_tmp = line.delete("<").delete(">").split(nil, 3)
  
  # When uri_o is not a URI but a literal
  if m = uri_o_tmp.match(/(".*").*/)
    uri_o = m[1]
  # When uri_o is a URI
  else
    uri_o = uri_o_tmp.split(nil)[0]
  end

  # Go to the next triple if empty
  next if uri_s.empty? || uri_p.empty? || uri_o.empty?

  # If uri_o is not a URI, but a value, add a property.
  if value_o = uri_o.match(/"(.*)".*/)
    # Create or get node id for s node
    node_s = get_or_create_node(conn, uri_s)
    # Add property
    puts uri_s, uri_o
    add_property(conn, node_s, uri_o, uri_p)
  # Otherwise, uri_o is a URI, so create its node and a relationship.
  else
    # Create or get node id for s node and o node
    node_s = get_or_create_node(conn, uri_s)
    node_o = get_or_create_node(conn, uri_o)
    # Create relationship
    get_or_create_relationship(conn, node_s, node_o, uri_p)
  end

  puts "  #{count} triples loaded" if (count += 1) % 100 == 0
end

puts "  #{$load_n} nodes loaded"
puts "  #{$skip_n} nodes skipped"
puts "  #{$load_r} relationships loaded"
puts "  #{$skip_r} relationships skipped"
puts "  #{$load_p} properties loaded"
puts "  #{$skip_p} properties skipped"
puts "  #{$warn_p} properties warning"

puts "done!"
