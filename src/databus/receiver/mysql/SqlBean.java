package databus.receiver.mysql;

import databus.receiver.Bean;

public interface SqlBean extends Bean {

    String toSql(Object otherInfo);
}

