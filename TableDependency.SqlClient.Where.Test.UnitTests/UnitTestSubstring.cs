using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.Test.UnitTests.Models;

namespace TableDependency.SqlClient.Where.Test.UnitTests
{
    [TestClass]
    public class UnitTestSubstring
    {
        [TestMethod]
        public void SubstringTests1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Substring(0, 3) == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(SUBSTRING([Code], 0, 3) = '123')", where);
        }
    }
}
