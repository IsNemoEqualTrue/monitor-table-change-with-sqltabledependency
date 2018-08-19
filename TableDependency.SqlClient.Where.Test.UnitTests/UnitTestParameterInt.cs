using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.Test.UnitTests.Models;

namespace TableDependency.SqlClient.Where.Test.UnitTests
{
    [TestClass]
    public class UnitTestParameterInt
    {
        [TestMethod]
        public void UnitTestParameters1()
        {
            var par1 = 123;

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Id == par1;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Id] = 123)", where);
        }

        [TestMethod]
        public void UnitTestParameters2()
        {
            var par1 = 123;

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Id >= par1;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Id] >= 123)", where);
        }

        [TestMethod]
        public void UnitTestParameters3()
        {
            var par1 = 123;
            var par2 = 321;

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Id >= par1 && p.Id <= par2;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(([Id] >= 123) AND ([Id] <= 321))", where);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void UnitTestParameters4()
        {
            var par1 = "123";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Id >= Int32.Parse(par1);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert            
        }
    }
}