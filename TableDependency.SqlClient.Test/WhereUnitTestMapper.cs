using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereUnitTestMapper
    {
        [TestMethod]
        public void Mapping1()
        {
            var mapper = new ModelToTableMapper<Product>();
            mapper.AddMapping(c => c.Code, "BarCode");

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code == "042100005264";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression, mapper).Translate();

            // Assert
            Assert.AreEqual("([BarCode] = '042100005264')", where);
        }

        [TestMethod]
        public void Mapping2()
        {
            var mapper = new ModelToTableMapper<Product>();
            mapper.AddMapping(c => c.Code, "[BarCode]");

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code == "042100005264";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression, mapper).Translate();

            // Assert
            Assert.AreEqual("([BarCode] = '042100005264')", where);
        }
    }
}