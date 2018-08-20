using System;
using System.Linq;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

using Expression = NCalc.Expression;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereUnitTestMoreCondition
    {
        [TestMethod]
        public void MoreConditions1()
        {
            var ids = new[] { 1, 2, 3 };

            // Arrange
            Expression<Func<Product, bool>> expression = p => ids.Contains(p.Id) && p.Code.Trim().Substring(0, 3).Equals("WWW");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Id] IN (1,2,3) AND SUBSTRING(LTRIM(RTRIM([Code])), 0, 3) = 'WWW')", where);
        }

        [TestMethod]
        public void MoreConditions2()
        {
            var ids = new[] { 1, 2, 3 };

            // Arrange
            Expression<Func<Product, bool>> expression = p => 
                ids.Contains(p.Id) && 
                p.Code.Trim().Substring(0, 3).Equals("WWW") && 
                p.Id == 100;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(([Id] IN (1,2,3) AND SUBSTRING(LTRIM(RTRIM([Code])), 0, 3) = 'WWW') AND ([Id] = 100))", where);
        }

        [TestMethod]
        public void MoreConditions3()
        {
            var ids = new[] { 1 };

            // Arrange
            Expression<Func<Product, bool>> expression = p => 
                ids.Contains(p.Id) || 
                (p.Code.Equals("WWW") && p.Code.Substring(0, 3) == "22");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Id] IN (1) OR ([Code] = 'WWW' AND (SUBSTRING([Code], 0, 3) = '22')))", where);
        }

        [TestMethod]
        public void MoreConditions4()
        {
            var ids = new[] { 1 };

            // Arrange
            Expression<Func<Product, bool>> expression = p => 
                ids.Contains(p.Id) || 
                p.Code.Equals("WWW") && 
                p.Code.Substring(0, 3) == "22" ||
                p.ExcangeRate > 1;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(([Id] IN (1) OR ([Code] = 'WWW' AND (SUBSTRING([Code], 0, 3) = '22'))) OR ([ExcangeRate] > 1))", where);
        }
        
        [TestMethod]
        public void MoreConditions5()
        {
            // 1 OR 0 AND 0 => 0
            // (1 OR 0) AND 0 => 0
            // 1 OR (0 AND 0) => 1

            // Arrange
            Expression<Func<Product, bool>> expression1 = p => p.Id == 1 || p.Id == 0 && p.Id == 0;
            Expression<Func<Product, bool>> expression2 = p => (p.Id == 1 || p.Id == 0) && p.Id == 0;
            Expression<Func<Product, bool>> expression3 = p => p.Id == 1 || (p.Id == 0 && p.Id == 0);

            // Act
            var where1 = new SqlTableDependencyFilter<Product>(expression1).Translate();
            var where2 = new SqlTableDependencyFilter<Product>(expression2).Translate();
            var where3 = new SqlTableDependencyFilter<Product>(expression3).Translate();

            // Assert
            Assert.AreEqual("(([Id] = 1) OR (([Id] = 0) AND ([Id] = 0)))", where1);
            Assert.AreEqual("((([Id] = 1) OR ([Id] = 0)) AND ([Id] = 0))", where2);
            Assert.AreEqual("(([Id] = 1) OR (([Id] = 0) AND ([Id] = 0)))", where3);

            var expr1 = where1.Replace("[Id] = ", string.Empty).Replace("1", "true").Replace("0", "false").Replace("OR", "||").Replace("AND", "&&");
            var result1 = new Expression(expr1).Evaluate();
            Assert.AreEqual(true, result1);

            var expr2 = where2.Replace("[Id] = ", string.Empty).Replace("1", "true").Replace("0", "false").Replace("OR", "||").Replace("AND", "&&");
            var result2 = new Expression(expr2).Evaluate();
            Assert.AreEqual(false, result2);

            var expr3 = where3.Replace("[Id] = ", string.Empty).Replace("1", "true").Replace("0", "false").Replace("OR", "||").Replace("AND", "&&");
            var result3 = new Expression(expr3).Evaluate();
            Assert.AreEqual(true, result3);
        }
    }
}