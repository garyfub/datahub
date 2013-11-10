package DataHubORMTests;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import DataHub.DHCell;
import DataHub.DHConnection;
import DataHub.DHConnectionParams;
import DataHub.DHConnectionParams._Fields;
import DataHub.DHDatabase;
import DataHub.DHException;
import DataHub.DHQueryResult;
import DataHub.DHRow;
import DataHub.DataHub;

public class ModelBasicTests {
	

}
