package com.sankuai.inf.leaf.server;

import com.alibaba.druid.pool.DruidDataSource;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.impl.IDAllocDaoImpl;
import com.sankuai.inf.leaf.server.exception.InitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Properties;

@Service("SegmentService")
public class SegmentService {
    private Logger logger = LoggerFactory.getLogger(SegmentService.class);
    IDGen idGen;
    DruidDataSource dataSource;
    public SegmentService() throws SQLException, InitException {
        Properties properties = PropertyFactory.getProperties();
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SEGMENT_ENABLE, "true"));
        if (flag) {


            // Config dataSource
            // ?serverTimezone=Shanghai&?useUnicode=true&characterEncoding=utf8&useSSL=false
            dataSource = new DruidDataSource();
            dataSource.setUrl(properties.getProperty(Constants.LEAF_JDBC_URL));
            dataSource.setInitialSize(10);
            dataSource.setMaxActive(500);
            dataSource.setMaxWait(10000);
            dataSource.setTimeBetweenEvictionRunsMillis(80000);
            dataSource.setMinEvictableIdleTimeMillis(60000);
            dataSource.setValidationQuery("SELECT 1");
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnReturn(false);
            dataSource.setPoolPreparedStatements(false);
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(300);
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setTestWhileIdle(true);
            dataSource.setUsername(properties.getProperty(Constants.LEAF_JDBC_USERNAME));
            dataSource.setPassword(properties.getProperty(Constants.LEAF_JDBC_PASSWORD));
            dataSource.init();

            // Config Dao
            IDAllocDao dao = new IDAllocDaoImpl(dataSource);

            // Config ID Gen
            idGen = new SegmentIDGenImpl();
            ((SegmentIDGenImpl) idGen).setDao(dao);
            if (idGen.init()) {
                logger.info("Segment Service Init Successfully");
            } else {
                throw new InitException("Segment Service Init Fail");
            }
        } else {
            idGen = new ZeroIDGen();
            logger.info("Zero ID Gen Service Init Successfully");
        }
    }
    public Result getId(String key) {
        return idGen.get(key);
    }
    public SegmentIDGenImpl getIdGen() {
        if (idGen instanceof SegmentIDGenImpl) {
            return (SegmentIDGenImpl) idGen;
        }
        return null;
    }
}
