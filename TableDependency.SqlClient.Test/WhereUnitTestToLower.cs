using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereUnitTestToLower
    {
        [TestMethod]
        public void ToLower1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.ToLower() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(LOWER([Code]) = '123')", where);
        }
    }
}