import cassandra
# from cassandra import DriverException
from cassandra.cluster import Cluster
# from cassandra.protocol import ProtocolHandler
# from cassandra.protocol import read_int
# from cassandra.protocol import ResultMessage
import time
import uuid


noop_from_binary = lambda x,y: x


# class MyFastResultMessage(ResultMessage):

#     @classmethod
#     def recv_results_rows(cls, f, protocol_version, user_type_map, result_metadata):
#         # replace deserialization from_binary with noop
#         paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)
#         column_metadata = column_metadata or result_metadata
#         rowcount = read_int(f)
#         rows = [cls.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
#         colnames = [c[2] for c in column_metadata]
#         coltypes = [c[3] for c in column_metadata]
#         try:
#             parsed_rows = [
#                 tuple(noop_from_binary(val, protocol_version)
#                       for ctype, val in zip(coltypes, row))
#                 for row in rows]
#         except Exception:
#             for i in range(len(row)):
#                 try:
#                     noop_from_binary(row[i], protocol_version)
#                 except Exception as e:
#                     raise DriverException('Failed decoding result column "%s" of type %s: %s' % (colnames[i],
#                                                                                                  coltypes[i].cql_parameterized_type(),
#                                                                                                  e.message))
#         return paging_state, (colnames, parsed_rows)


# class MyProtocolHandler(ProtocolHandler):
#     my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
#     my_opcodes[ResultMessage.opcode] = MyFastResultMessage
#     message_types_by_opcode = my_opcodes


def consume_results(res):
    return [_ for _ in res]

def consume_results_and_deserialize_uuid(res):
    return [uuid.UUID(bytes=r.activity_id) for r in res]

def run_bench(session, result_consumer, runs=1000):
    query = '''SELECT "activity_id" FROM test.test LIMIT 1600;'''
    fulltrip_elapsed_times = []

    statement = cassandra.query.SimpleStatement(query)
    [_ for _ in session.execute(statement)]

    for i in range(runs):
        t0 = time.time()
        result_consumer(session.execute(statement))
        d = (time.time() - t0) * 1000
        fulltrip_elapsed_times.append(d)

    print('AVG: %sms' % (sum(fulltrip_elapsed_times)/len(fulltrip_elapsed_times)))
    print('MAX: %sms' % max(fulltrip_elapsed_times))
    print('MIN: %sms' % min(fulltrip_elapsed_times))
    print('SUM: %sms' % sum(fulltrip_elapsed_times))

print('Round-trip timings with default C* protocol handler and deserialization:')
print('---------------')
cluster = Cluster(['127.0.0.1'])
# cluster.protocol_version = 3
session = cluster.connect()
run_bench(session, consume_results)
session.shutdown()

# print('Round-trip timings with dummy C* protocol handler and manual deserialization:')
# print('---------------')
# cluster = Cluster(['127.0.0.1'])
# cluster.protocol_version = 3
# session = cluster.connect()
# session.client_protocol_handler = MyProtocolHandler
# run_bench(session, consume_results_and_deserialize_uuid)
