#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.util.Bytes

# check the number of arguments passed
if ARGV.length < 4
    puts "Usage: hbase org.jruby.Main read_data.rb <table> <column-family> <column> <outfile>"
    exit 1
end

tablename = ARGV[0]
columnfam = ARGV[1]
colnames = ARGV[2]
outfile = ARGV[3]

columnFamily = columnfam.to_java_bytes
cnlist = colnames.split(',')
cnblist = Array.new
cnlist.each { |x| cnblist << x.to_java_bytes }

conf = HBaseConfiguration.create()
table = HTable.new conf, tablename.to_java_bytes
scan = Scan.new
cnblist.each { |x| scan.addColumn columnFamily, x }
scanner = table.getScanner(scan)

File.open(outfile, 'w') do |of|
    iter = scanner.iterator
    while iter.hasNext
        result = iter.next
        rout = Array.new
        cnblist.each do |x|
            out = result.getValue columnFamily, x
            rout << Bytes.toString(out)
        end
        of.puts rout.join(" ")
    end
    scanner.close
end
