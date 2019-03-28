

import UIKit
import FMDB

let dbManager = DBManager()

class DBManager: NSObject {
    
    // MARK: - init

    var db:FMDatabase!
    
    @objc class var sharedManager : DBManager {
        return dbManager
    }
    
    fileprivate override init() {
        let path = ConsDocumentPath + "db.sqlite"
        
        db = FMDatabase(path: path)
        
        Log.debug("db --- path: " + path)
    }
    
    // MARK: - private

    /// 执行sql语句
    ///
    /// - Parameter sql: sql
    /// - Returns: 结果
    func executeUpdate(sql:String,values:[Any]?) -> Bool{
        var res = false

        if db.open() {
            do{
                try db.executeUpdate(sql, values: values)
                print("sql: " + sql + " --- successed！")
                res = true
            }catch{
                print(db.lastErrorMessage())
            }
            
            db.close()

        }
        else{
            print("db open failed!")
        }

    
        return res
    }
    
    /// 执行查询
    ///
    /// - Parameters:
    ///   - sql: sql
    ///   - values: values
    /// - Returns:FMResultSet
    func executeQuery(sql:String,values:[Any]?,closure:((FMResultSet?) -> Void)? = nil) -> FMResultSet?{
        if db.open() {
            do{
                let res = try db.executeQuery(sql, values: values)
                print("sql: " + sql + " --- successed！")
                
                if closure != nil{
                    closure!(res)
                }
                
                return res
                
            }catch{
                print(db.lastErrorMessage())
                return nil

            }
        }
        else{
            print("db open failed!")
            return nil

        }
    }
    
    /// 判断表是否存在
    ///
    /// - Parameter name: 表名
    /// - Returns: bool值
    func tableIsExist(name:String) -> Bool{
        let sql = "select count(*) as 'count' from sqlite_master where type ='table' and name = ?"
        
        
        var result = false
        
        
        let rs = executeQuery(sql: sql, values: [name])
        
        if rs == nil {
            result = false
        }
        
        while rs!.next() {
            let count = rs!.int(forColumn: "count")
            if count == 0{
                print("table: \(name) no exist")
                result = false
            }
            else{
                result = true
            }
        }
        rs?.close()
        
        return result
    }
    
    // MARK: - public

    
    /// 建表
    ///
    /// - Parameters:
    ///   - tableName: 表名
    ///   - fields: ["name","age"]
    func createTable(tableName:String,fields:[String]){
        if tableIsExist(name: tableName){
            return
        }
        
        var  sql = "CREATE TABLE IF NOT EXISTS " + " '\(tableName)' " + "(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
        fields.forEach { (field) in
            sql = sql + field  + " TEXT,"
        }
        
        sql.removeLast()
        
        sql = sql + ")"
        
        _ = executeUpdate(sql: sql,values: nil)
        
    }
    
    /// 删除表
    ///
    /// - Parameter tableName: 表名
    func dropTable(tableName:String){
        
        if !tableIsExist(name: tableName){
            return
        }
        
        let sql = "DROP TABLE " + tableName
        _ = executeUpdate(sql: sql,values: nil)

    }
    
    /// 更新数据
    ///
    /// - Parameters:
    ///   - table: 表名
    ///   - fields: ["name":"lijun","age":"1"]
    ///   - condition: where ["age":"2"]
    /// - Returns: bool 是否成功
    func update(tableName:String,fields:[String:String],conditions:[String:String]? = nil) -> Bool{
        if !tableIsExist(name: tableName){
            return false
        }
        var res = false
        var values:[String] = []
        var sql = "UPDATE " + "\(tableName)" +  " SET "
        
        fields.forEach { (field) in
            sql = sql + field.key + "= ?,"
            values.append(field.value)
        }
        
        sql.removeLast()
        
        if conditions != nil {
            sql = sql + " WHERE "
            
            conditions!.forEach { (condition) in
                sql = sql + condition.key + " = ? and "
                values.append(condition.value)
            }
            
            let range = sql.index(sql.endIndex, offsetBy: -4)..<sql.endIndex
            
            sql.removeSubrange(range)
        }
        
        
        res = executeUpdate(sql: sql, values: values)
        
        return res
    }
    
    
    
    /// 查询
    ///
    /// - Parameters:
    ///   - tableName: 表名
    ///   - fields: ["name","age"]
    ///   - conditions: ["age":"1"]
    /// - Returns: [String:String]
    func query(tableName:String,fields:[String],conditions:[String:String]? = nil) -> [[String:String]]{
        if !tableIsExist(name: tableName){
            return []
        }
        var sql = "SELECT"
        var values:[String] = []
        
        fields.forEach { (field) in
            sql = sql + " \(field),"
        }
        sql.removeLast()
        
        sql = sql + " FROM " + "'\(tableName)'"
        
        
        if conditions != nil {
            sql = sql + " WHERE "
            
            conditions!.forEach { (condition) in
                sql = sql + condition.key + " = ? and "
                values.append(condition.value)
            }
            
            let range = sql.index(sql.endIndex, offsetBy: -4)..<sql.endIndex
            
            sql.removeSubrange(range)
        }
        
        var result:[[String:String]] = []
        
        let rs = executeQuery(sql: sql, values: values)
        
        if rs != nil {
            while rs!.next() {
                fields.forEach { (field) in
                    let item = rs?.string(forColumn: field)
                    result.append([field:item!])
                }
            }
        }

        rs?.close()
        
        return result
        
    }
    
    func insert(tableName:String,fields:[String:String]) -> Bool{
        if !tableIsExist(name: tableName){
            return false
        }
        var res = false
        var values:[String] = []
        var sql = "INSERT INTO " + tableName + " ("
        
        var sqlUpdateLast = " VALUES("

        
        fields.forEach { (field) in
            sql = sql + field.key + ","
            sqlUpdateLast = sqlUpdateLast + "?,"
            values.append(field.value)
        }
        sql.removeLast()
        sqlUpdateLast.removeLast()
        
        sql = sql + ")"
        sqlUpdateLast = sqlUpdateLast + ")"
        
        sql = sql + sqlUpdateLast
        
        res = executeUpdate(sql: sql, values: values)
        
        return res
    }
    
    /// 删除
    ///
    /// - Parameters:
    ///   - tableName: 表名
    ///   - conditions: ["name":"lijun"]
    /// - Returns: Bool
    func delete(tableName:String,conditions:[String:String]) -> Bool{
        if !tableIsExist(name: tableName){
            return false
        }
        var res = false

        var values:[String] = []
        
        var sql = "DELETE FROM '" + tableName + "' WHERE "
        conditions.forEach { (condition) in
            sql = sql + condition.key + " = ? and "
            values.append(condition.value)
        }
        
        let range = sql.index(sql.endIndex, offsetBy: -4)..<sql.endIndex
        sql.removeSubrange(range)
        
        res = executeUpdate(sql: sql, values: values)
        
        return res
    }
    
}
