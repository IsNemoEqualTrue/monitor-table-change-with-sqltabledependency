using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.Tests.Models;

namespace TableDependency.SqlClient.Where.Tests
{
    [TestClass]
    public class UnitTestMapper
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