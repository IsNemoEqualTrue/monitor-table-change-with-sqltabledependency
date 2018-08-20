using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereUnitTestSubstring
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
