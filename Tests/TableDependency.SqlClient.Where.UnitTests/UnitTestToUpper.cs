using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.SqlClient.Where.Tests.Models;

namespace TableDependency.SqlClient.Where.Tests
{
    [TestClass]
    public class UnitTestToUpper
    {
        [TestMethod]
        public void ToUpper1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.ToUpper() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(UPPER([Code]) = '123')", where);
        }
    }
}