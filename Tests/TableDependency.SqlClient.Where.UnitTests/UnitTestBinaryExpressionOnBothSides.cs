using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.UnitTests.Models;

namespace TableDependency.SqlClient.Where.UnitTests
{
    [TestClass]
    public class UnitTestBinaryExpressionOnBothSides
    {
        [TestMethod]
        public void ExpressionOnBothSides1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Substring(1,3).ToLower() == p.Id.ToString().ToLower();

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(LOWER(SUBSTRING([Code], 1, 3)) = LOWER(CONVERT(varchar(MAX), [Id])))", where);
        }
    }
}
