package com.test.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * @author wangjunwei
 * @date 2020/9/25 10:17
 */
object JDBCUtil {

  def getConnection(props: Properties): Connection = {
    val url = props.getProperty("url")
    val driver = props.getProperty("driver")
    val user = props.getProperty("user")
    val password = props.getProperty("password")
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }

  def buildJdbcProps(props: Properties): Properties = {
    val url = props.getProperty("url")
    val driver = props.getProperty("driver")
    val user = props.getProperty("user")
    val password = props.getProperty("password")
    val jdbcProps = new Properties()
    jdbcProps.put("url", url)
    jdbcProps.put("driver", driver)
    jdbcProps.put("user", user)
    jdbcProps.put("password", password)
    jdbcProps
  }
}
