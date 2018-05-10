using System;
using System.Linq.Expressions;

namespace TableDependency.SqlClient.Where.Development.Utils
{
    public class DisplayVisitor : ExpressionVisitor
    {
        private int _level;

        public override Expression Visit(Expression exp)
        {
            if (exp != null)
            {
                for (int i = 0; i < _level; i++)
                {
                    Console.Write(" ");
                }
                Console.WriteLine("{0} - {1}", exp.NodeType, exp.GetType().Name);
            }

            _level++;
            Expression result = base.Visit(exp);
            _level--;
            return result;
        }

        public void Display(Expression exp)
        {
            Console.WriteLine("===== DisplayVisitor.Display =====");
            this.Visit(exp);
        }
    }
}
