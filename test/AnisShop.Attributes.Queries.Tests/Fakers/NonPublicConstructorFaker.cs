using Bogus;
using System.Reflection;

namespace AnisShop.Attributes.Queries.Tests.Fakers
{
    public abstract class NonPublicConstructorFaker<T> : Faker<T> where T : class
    {
        protected NonPublicConstructorFaker()
        {
            CustomInstantiator(f => CreateInstance());
        }

        private T CreateInstance()
        {
            var type = typeof(T);
            var constructors = type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public);

            if (constructors.Length == 0)
            {
                throw new InvalidOperationException($"No suitable constructor found for type {type.Name}");
            }

            var constructor = constructors[0];
            var parameters = constructor.GetParameters();
            var parameterValues = new object[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                var paramType = parameters[i].ParameterType;
                parameterValues[i] = GetDefaultValue(paramType)!;
            }

            return (T)constructor.Invoke(parameterValues);
        }

        private static object? GetDefaultValue(Type type)
        {
            if (type == typeof(string))
                return string.Empty;

            if (type.IsValueType)
                return Activator.CreateInstance(type);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return null;

            return null;
        }
    }
}
