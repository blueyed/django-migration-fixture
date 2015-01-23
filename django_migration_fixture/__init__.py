import os

from django.core import serializers
from django.core.management.color import no_style
from django.db import connection
from django.db.models import signals


class FixtureObjectDoesNotExist(Exception):
    """Raised when attempting to roll back a fixture and the instance can't be found"""
    pass


def reset_db_sequences(models):
    if not models:
        return
    with connection.cursor() as cursor:
        for sql in connection.ops.sequence_reset_sql(no_style(), models):
            cursor.execute(sql)


def fixture(app, fixtures, fixtures_dir='fixtures', raise_does_not_exist=False):
    """
    Load fixtures using a data migration.

    Usage:

    import myapp
    import anotherapp

    operations = [
        migrations.RunPython(**fixture(myapp, 'eggs.yaml')),
        migrations.RunPython(**fixture(anotherapp, ['sausage.json', 'walks.yaml']))
    ]
    """
    fixture_path = os.path.join(app.__path__[0], fixtures_dir)
    if isinstance(fixtures, basestring):
        fixtures = [fixtures]

    def get_format(fixture):
        return os.path.splitext(fixture)[1][1:]

    def get_objects():
        for fixture in fixtures:
            with open(os.path.join(fixture_path, fixture), 'rb') as f:
                objects = serializers.deserialize(get_format(fixture), f, ignorenonexistent=True)
                for obj in objects:
                    yield obj

    def load_fixture(app_config, schema_editor):
        """Entrypoint for RunPython to load the fixture.

        This delays the actual loading until this app itself is being called
        through the post_migrate signal hook.  This is necessary for
        the contenttypes and auth data to be available.

        After the fixture has been loaded the database sequences for affected
        models are being 'reset' (using `coalesce` with PostgreSQL)."""

        def signal_handler(app_config, sender, **kwargs):
            if sender.label == "django_migration_fixture":
                assert app_config.label == "django_migration_fixture"

                models = set()
                for obj in get_objects():
                    obj.save()
                    models.add(obj.object._meta.model)

                reset_db_sequences(models)

                # Disconnect the signal, otherwise we might be called during
                # e.g. flush (in tests) again.
                signals.post_migrate.disconnect(signal_handler, weak=False)

        signals.post_migrate.connect(signal_handler, weak=False)

    def unload_fixture(apps, schema_editor):
        for obj in get_objects():
            model = apps.get_model(app.__name__, obj.object.__class__.__name__)
            kwargs = dict()
            if 'id' in obj.object.__dict__:
                kwargs.update(id=obj.object.__dict__.get('id'))
            elif 'slug' in obj.object.__dict__:
                kwargs.update(slug=obj.object.__dict__.get('slug'))
            else:
                kwargs.update(**obj.object.__dict__)
            try:
                model.objects.get(**kwargs).delete()
            except model.DoesNotExist:
                if not raise_does_not_exist:
                    raise FixtureObjectDoesNotExist("Model %s instance with kwargs %s does not exist." % (model, kwargs))

    return dict(code=load_fixture, reverse_code=unload_fixture)
