"""

Examples illustrating the usage of the "association object" pattern,
where an intermediary object associates two endpoint objects together.

The first example illustrates a basic association from a User object
to a collection or Order objects, each which references a collection of Item objects.

The second example builds upon the first to add the Association Proxy extension.

E.g.::

    # create an order
    order = Order('john smith')

    # append an OrderItem association via the "itemassociations"
    # collection with a custom price.
    order.itemassociations.append(OrderItem(item('MySQL Crowbar'), 10.99))

    # append two more Items via the transparent "items" proxy, which
    # will create OrderItems automatically using the default price.
    order.items.append(item('SA Mug'))
    order.items.append(item('SA Hat'))

"""