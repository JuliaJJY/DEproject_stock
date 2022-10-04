from rest_framework import serializers
from rest_api.models import *
from rest_api.serializers import * 
# class UserSerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = User
#         fields = ['url', 'username', 'email', 'groups']

class CoFacilitySerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = CoFacility
        fields = ['url', 'fac_popu', 'qur_rate', 'std_day']


class CoPopuDensitySerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = CoPopuDensity
        fields = ['url', 'popu_density', 'qur_rate', 'std_day']


class CoVaccineSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = CoVaccine
        fields = ['loc', 'v1th_rate', 'v2th_rate', 'v3th_rate','v4th_rate','qur_rate','std_day']


class CoWeekdaySerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = CoWeekday
        fields = ['sun', 'mon', 'tue', 'wed','thu','fri','sat','std_day']


